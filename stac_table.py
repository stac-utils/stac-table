"""
Generate STAC Collections for tabular datasets.
"""
__version__ = "1.0.0"
import json
import copy
import pystac
import pyarrow
import pyarrow.parquet
import fsspec
from typing import TypeVar, Union


T = TypeVar("T", pystac.Collection, pystac.Item)
SCHEMA_URI = "https://stac-extensions.github.io/table/v1.0.0/schema.json"


def generate(
    ds: Union[str, pyarrow.parquet.ParquetDataset], template, storage_options=None
) -> T:
    """
    Generate a STAC Item from a Parquet Dataset.

    Parameters
    ----------
    ds : pyarrow.parquet.ParquetDataset or str
        The input table to generate a STAC item for. For `str` input, this is
        treated as an fsspec-compatible URI. The fsspec filesystem is loaded and
        the ParquetDataset is loaded using that filesystem.

        If providing as a `pyarrow.parquet.ParquetDataset`, make sure to use
        ``use_legacy_dataset=False``.
    template : pystac.Item
        The template item. This will be cloned and new data will be filled in.

    Returns
    -------
    pystac.Item
        The updated pystac.Item with the following fields set

        * stac_extensions : added `table` extension
        * table:columns
        * table:geo_arrow_metadata
    """
    # TODO: compute geometry / bbox from the geometry column
    # TODO: update stac_extensions
    template = copy.deepcopy(template)

    if isinstance(ds, str):
        storage_options = storage_options or {}
        ds = parquet_dataset_from_url(ds, storage_options)

    columns = generate_columns(ds)
    geo_arrow_metadata = generate_geo_arrow_metadata(ds)

    template.properties["table:columns"] = columns
    template.properties["table:geo_arrow_metadata"] = geo_arrow_metadata

    if SCHEMA_URI not in template.stac_extensions:
        template.stac_extensions.append(SCHEMA_URI)

    return template


def generate_columns(ds: pyarrow.parquet.ParquetDataset) -> list:
    columns = []
    for field in ds.schema:
        column = {"name": field.name, "metadata": field.metadata}
        columns.append(column)
    return columns


def generate_geo_arrow_metadata(ds):
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
