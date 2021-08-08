"""
Generate STAC Collections for tabular datasets.
"""
__version__ = "1.0.0"
import json
import copy
import pystac
import pyarrow.parquet
from typing import TypeVar


T = TypeVar("T", pystac.Collection, pystac.Item)


def generate(ds: pyarrow.parquet.ParquetDataset, template) -> T:
    template = copy.deepcopy(template)

    columns = generate_columns(ds)
    geo_arrow_metadata = generate_geo_arrow_metadata(ds)

    template.properties["table:columns"] = columns
    template.properties["table:geo_arrow_metadata"] = geo_arrow_metadata

    return template


def generate_columns(ds: pyarrow.parquet.ParquetDataset) -> list:
    columns = []
    for field in ds.schema:
        column = {
            "name": field.name,
            "metadata": field.metadata,
        }
        columns.append(column)
    return columns


def generate_geo_arrow_metadata(ds):
    return json.loads(ds.schema.metadata[b"geo"])