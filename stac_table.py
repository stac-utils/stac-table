"""
Generate STAC Collections for tabular datasets.
"""
__version__ = "1.0.0"
import copy
import enum
from typing import TypeVar

import dask
import dask_geopandas
import fsspec
import pandas as pd
import pyarrow as pa
import pyproj
import pystac
import shapely.geometry
from shapely.ops import transform

T = TypeVar("T", pystac.Collection, pystac.Item)
SCHEMA_URI = "https://stac-extensions.github.io/table/v1.2.0/schema.json"
# https://issues.apache.org/jira/browse/PARQUET-1889: parquet doesn't officially
# have a type yet.
PARQUET_MEDIA_TYPE = "application/x-parquet"


class InferDatetimeOptions(str, enum.Enum):
    no = "no"
    midpoint = "midpoint"
    unique = "unique"
    range = "range"


def generate(
    uri: str,
    template,
    infer_bbox=None,
    infer_geometry=False,
    datetime_column=None,
    infer_datetime=InferDatetimeOptions.no,
    count_rows=True,
    asset_key="data",
    asset_extra_fields=None,
    proj=True,
    storage_options=None,
    validate=True,
) -> T:
    """
    Generate a STAC Item from a Parquet Dataset.

    Parameters
    ----------
    uri : str
        The fsspec-compatible URI pointing to the input table to generate a
        STAC item for.
    template : pystac.Item
        The template item. This will be cloned and new data will be filled in.
    infer_bbox : str, optional
        The column name to use setting the Item's bounding box.

        .. note::

           If the dataset doesn't provide spatial partitions, this will
           require computation.

    infer_geometry: bool, optional
        Whether to fill the item's `geometry` field with the union of the
        geometries in the `infer_bbox` column.

    datetime_column: str, optional
        The column name to use when setting the Item's `datetime` or
        `start_datetime` and `end_datetime` properties. The method used is
        determined by `infer_datetime`.

    infer_datetime: str, optional.
        The method used to find a datetime from the values in `datetime_column`.
        Use the options in the `InferDatetimeOptions` enum.

        - no : do not infer a datetime
        - midpoint : Set `datetime` to the midpoint of the highest and lowest values.
        - unique : Set `datetime` to the unique value. Raises if more than one
          unique value is found.
        - range : Set `start_datetime` and `end_datetime` to the minimum and
          maximum values.

    count_rows : bool, default True
        Whether to add the row count to `table:row_count`.

    asset_key : str, default "data"
        The asset key to use for the parquet dataset. The href will be the ``uri`` and
        the roles will be ``["data"]``.

    asset_extra_fields : dict, optional
        Additional fields to set in the asset's ``extra_fields``.

    proj : bool or dict, default True
        Whether to extract projection information from the dataset and store it
        using the `projection` extension.

        By default, just `proj:crs` is extracted. If `infer_bbox` or `infer_geometry`
        are specified, those will be set as well.

        Alternatively, provide a dict of values to include.

    storage_options: mapping, optional
        A dictionary of keywords to provide to :meth:`fsspec.get_fs_token_paths`
        when creating an fsspec filesystem with a str ``ds``.

    validate : bool, default True
        Whether to validate the returned pystac.Item.

    Returns
    -------
    pystac.Item
        The updated pystac.Item with the following fields set

        * stac_extensions : added `table` extension
        * table:columns

    Examples
    --------

    This example generates a STAC item based on the "naturalearth_lowres" datset
    from geopandas. There's a bit of setup.

    >>> import datetime, geopandas, pystac, stac_table
    >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
    >>> gdf.to_parquet("data.parquet")

    Now we can create the item.

    >>> # Create the template Item
    >>> item = pystac.Item(
    ...     "naturalearth_lowres",
    ...     geometry=None,
    ...     bbox=None,
    ...     datetime=datetime.datetime(2021, 1, 1),
    ...     properties={},
    ... )
    >>> result = stac_table.generate("data.parquet", item)
    >>> result
    <Item id=naturalearth_lowres>
    """
    template = copy.deepcopy(template)

    data = None
    storage_options = storage_options or {}
    # data = dask_geopandas.read_parquet(
    #     ds, storage_options=storage_options
    # )
    ds = parquet_dataset_from_url(uri, storage_options)

    if (
        infer_bbox
        or infer_geometry
        or infer_datetime != InferDatetimeOptions.no
        or proj is True
    ):
        #     # TODO: this doesn't actually work
        #     data = dask_geopandas.read_parquet(
        #         ds.files, storage_options={"filesystem": ds.filesystem}
        #     )
        data = dask_geopandas.read_parquet(uri, storage_options=storage_options)
        if data.spatial_partitions is None:
            data.calculate_spatial_partitions()

    columns = get_columns(ds)
    template.properties["table:columns"] = columns

    if proj is True:
        proj = get_proj(data)
    proj = proj or {}

    # TODO: Add schema when published
    if SCHEMA_URI not in template.stac_extensions:
        template.stac_extensions.append(SCHEMA_URI)
    if proj and pystac.extensions.projection.SCHEMA_URI not in template.stac_extensions:
        template.stac_extensions.append(pystac.extensions.projection.SCHEMA_URI)

    extra_proj = {}
    if infer_bbox:
        src_crs = data.spatial_partitions.crs.to_epsg()
        tf = pyproj.Transformer.from_crs(src_crs, 4326, always_xy=True)

        bbox = data.spatial_partitions.unary_union.bounds
        # NOTE: bbox of unary union will be stored under proj extension as projected
        extra_proj["proj:bbox"] = bbox

        # NOTE: bbox will be stored in pystsac.Item.bbox in EPSG:4326
        bbox = transform(tf.transform, shapely.geometry.box(*bbox))
        template.bbox = bbox.bounds

    if infer_geometry:
        # NOTE: geom  under proj extension as projected
        geometry = data.unary_union.compute()
        extra_proj["proj:geometry"] = shapely.geometry.mapping(geometry)

        # NOTE: geometry will be stored in pystsac.Item.geometry in EPSG:4326
        src_crs = data.spatial_partitions.crs.to_epsg()
        tf = pyproj.Transformer.from_crs(src_crs, 4326, always_xy=True)
        geometry = transform(tf.transform, geometry)
        template.geometry = shapely.geometry.mapping(geometry)

    if infer_bbox and template.geometry is None:
        # If bbox is set then geometry must be set as well.
        template.geometry = shapely.geometry.mapping(
            shapely.geometry.box(*template.bbox)
        )

    if infer_geometry and template.bbox is None:
        template.bbox = shapely.geometry.shape(template.geometry).bounds

    if proj or extra_proj:
        template.properties.update(**extra_proj, **proj)

    if infer_datetime != InferDatetimeOptions.no and datetime_column is None:
        msg = "Must specify 'datetime_column' when 'infer_datetime != no'."
        raise ValueError(msg)

    if infer_datetime == InferDatetimeOptions.midpoint:
        values = dask.compute(data[datetime_column].min(), data[datetime_column].max())
        template.properties["datetime"] = pd.Series(values).mean().to_pydatetime()

    if infer_datetime == InferDatetimeOptions.unique:
        values = data[datetime_column].unique().compute()
        n = len(values)
        if n > 1:
            msg = f"infer_datetime='unique', but {n} unique values found."
            raise ValueError(msg)
        template.properties["datetime"] = values[0].to_pydatetime()

    if infer_datetime == InferDatetimeOptions.range:
        values = dask.compute(data[datetime_column].min(), data[datetime_column].max())
        values = list(pd.Series(values).dt.to_pydatetime())
        template.properties["start_datetime"] = values[0].isoformat() + "Z"
        template.properties["end_datetime"] = values[1].isoformat() + "Z"

    if count_rows:
        template.properties["table:row_count"] = sum(
            x.count_rows() for x in ds.fragments
        )

    if asset_key:
        asset = pystac.asset.Asset(
            uri,
            title="Dataset root",
            media_type=PARQUET_MEDIA_TYPE,
            roles=["data"],
            # extra_fields={"table:storage_options": asset_extra_fields},
            extra_fields=asset_extra_fields,
        )
        template.add_asset(asset_key, asset)

    if validate:
        template.validate()

    return template


def get_proj(ds):
    """
    Read projection information from the dataset.
    """
    # Use geopandas to get the proj info
    proj = {}
    maybe_crs = ds.geometry.crs
    if maybe_crs:
        maybe_epsg = ds.geometry.crs.to_epsg()
        if maybe_epsg:
            proj["proj:epsg"] = maybe_epsg
        else:
            proj["proj:wkt2"] = ds.geometry.crs.to_wkt()

    return proj


def get_columns(ds: pa.parquet.ParquetDataset) -> list:
    columns = []
    fragment = ds.fragments[0]

    for field, col in zip(ds.schema, fragment.metadata.schema):
        if field.name == "__null_dask_index__":
            continue

        column = {"name": field.name, "type": col.physical_type.lower()}
        if field.metadata is not None:
            column["metadata"] = field.metadata
        columns.append(column)
    return columns


def parquet_dataset_from_url(url: str, storage_options):
    fs, _, _ = fsspec.get_fs_token_paths(url, storage_options=storage_options)
    pa_fs = pa.fs.PyFileSystem(pa.fs.FSSpecHandler(fs))
    url2 = url.split("://", 1)[-1]  # pyarrow doesn't auto-strip the prefix.
    ds = pa.parquet.ParquetDataset(url2, filesystem=pa_fs, use_legacy_dataset=False)
    return ds
