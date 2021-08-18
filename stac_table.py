"""
Generate STAC Collections for tabular datasets.
"""
__version__ = "1.0.0"
import json
import copy
import enum
from pathlib import Path
from typing import TypeVar, Union

import dask
import pystac
import pandas as pd
import pyarrow
import pyarrow.parquet
import fsspec
import dask_geopandas
import shapely.geometry


T = TypeVar("T", pystac.Collection, pystac.Item)
SCHEMA_URI = "https://stac-extensions.github.io/table/v1.0.0/schema.json"
# https://issues.apache.org/jira/browse/PARQUET-1889: parquet doesn't officially have a type yet.
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
    asset="data",
    geo_arrow_metadata=True,
    storage_options=None,
) -> T:
    """
    Generate a STAC Item from a Parquet Dataset.

    Parameters
    ----------
    uri : str
        The fsspec-compatible URI pointing to the input table to generate a STAC item for.
    template : pystac.Item
        The template item. This will be cloned and new data will be filled in.
    infer_bbox : str, optional
        The column name to use setting the Item's bounding box.

        .. note::

           If the dataset doesn't provide spatial partitions, this will require computation.

    infer_geometry: bool, optional
        Whether to fill the item's `geometry` field with the union of the geometries in the
        `infer_bbox` column.

    datetime_column: str, optional
        The column name to use when setting the Item's `datetime` or `start_datetime`
        and `end_datetime` properties. The method used is determined by `infer_datetime`.

    infer_datetime: str, optional.
        The method used to find a datetime from the values in `datetime_column`.
        Use the options in the `InferDatetimeOptions` enum.

        - no : do not infer a datetime
        - midpoint : Set `datetime` to the midpoint of the highest and lowest values.
        - unique : Set `datetime` to the unique value. Raises if more than one unique value is found.
        - range : Set `start_datetime` and `end_datetime` to the minimum and maximum values.

    asset : str, default "data"
        The asset key to use for the parquet dataset. The asset will include
        the role ``["data"]``. Partitioned datasets will also include the
        role ``["root"]``.

    geo_arrow_metadata : bool or dict
        How to handle `geo_arrow_metadata`. By default, the dataset is assumed to include
        metadata compatible with `geo_arrow_spec`. You can provide a dict with your own
        metadata, or specify ``geo_arrow_metadata=False`` to skip adding this.

    storage_options: mapping, optional
        A dictionary of keywords to provide to :meth:`fsspec.get_fs_token_paths`
        when creating an fsspec filesystem with a str ``ds``.

    Returns
    -------
    pystac.Item
        The updated pystac.Item with the following fields set

        * stac_extensions : added `table` extension
        * table:columns
        * table:geo_arrow_metadata

    Examples
    --------

    This example generates a STAC item based on the "naturalearth_lowres" datset from geopandas.
    There's a bit of setup.

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

    if infer_bbox or infer_geometry or infer_datetime != InferDatetimeOptions.no:
        data = dask_geopandas.read_parquet(uri, storage_options=storage_options)
    #     # TODO: this doesn't actually work
    #     data = dask_geopandas.read_parquet(
    #         ds.files, storage_options={"filesystem": ds.filesystem}
    #     )

    columns = get_columns(ds)
    template.properties["table:columns"] = columns

    if geo_arrow_metadata is True:
        template.properties["table:geo_arrow_metadata"] = get_geo_arrow_metadata(ds)
    elif geo_arrow_metadata:
        template.properties["table:geo_arrow_metadata"] = geo_arrow_metadata

    if SCHEMA_URI not in template.stac_extensions:
        template.stac_extensions.append(SCHEMA_URI)

    if infer_bbox:
        template.bbox = data.spatial_partitions.unary_union.bounds

    if infer_geometry:
        template.geometry = shapely.geometry.mapping(data.unary_union.compute())

    if infer_datetime != InferDatetimeOptions.no and datetime_column is None:
        raise ValueError("Must specify 'datetime_column' when 'infer_datetime != no'.")

    if infer_datetime == InferDatetimeOptions.midpoint:
        values = dask.compute(data[datetime_column].min(), data[datetime_column].max())
        template.properties["datetime"] = pd.Series(values).mean().to_pydatetime()

    if infer_datetime == InferDatetimeOptions.unique:
        values = data[datetime_column].unique().compute()
        n = len(values)
        if n > 1:
            raise ValueError(f"infer_datetime='unique', but {n} unique values found.")
        template.properties["datetime"] = values[0].to_pydatetime()

    if infer_datetime == InferDatetimeOptions.range:
        values = dask.compute(data[datetime_column].min(), data[datetime_column].max())
        values = list(pd.Series(values).dt.to_pydatetime())
        template.properties["start_datetime"] = values[0]
        template.properties["end_datetime"] = values[1]

    if asset:
        # What's the best way to get the root of the ParquetDataset?
        files = ds.files
        if len(files) == 0:
            raise ValueError("Dataset %s has no files", ds)
        elif len(files) == 1:
            href = files[0]
            roles = ["data"]
        else:
            href = str(Path(files[0]).parent)
            roles = ["data", "root"]

        template.add_asset(
            asset,
            pystac.asset.Asset(
                href, title="Dataset root", media_type=PARQUET_MEDIA_TYPE, roles=roles
            ),
        )
        # TODO: https://github.com/TomAugspurger/stac-table/issues/1
        # Figure out if we want assets for each partition. IMO, they're redundant.

        # if len(files) > 1:
        #     for i, file in enumerate(files):
        #         template.add_asset(
        #             f"part.{i}",
        #             pystac.asset.Asset(
        #                 file,
        #                 title=f"Part {i}",
        #                 media_type=PARQUET_MEDIA_TYPE,
        #                 roles=["data", "part"],
        #             ),
        #         )

    return template


def get_columns(ds: pyarrow.parquet.ParquetDataset) -> list:
    columns = []
    for field in ds.schema:
        column = {"name": field.name}
        if field.metadata is not None:
            column["metadata"] = field.metadata
        columns.append(column)
    return columns


def get_geo_arrow_metadata(ds):
    if b"geo" not in ds.schema.metadata:
        raise ValueError(
            "Dataset must have a 'geo' metadata key, whose value is compatible with the geo-arrow-spec."
        )
    return json.loads(ds.schema.metadata[b"geo"])


def parquet_dataset_from_url(
    url: str, storage_options
) -> pyarrow.parquet._ParquetDatasetV2:
    fs, _, _ = fsspec.get_fs_token_paths(url, storage_options=storage_options)
    pa_fs = pyarrow.fs.PyFileSystem(pyarrow.fs.FSSpecHandler(fs))
    url2 = url.split("://", 1)[-1]  # pyarrow doesn't auto-strip the prefix.
    ds = pyarrow.parquet.ParquetDataset(
        url2, filesystem=pa_fs, use_legacy_dataset=False
    )
    return ds
