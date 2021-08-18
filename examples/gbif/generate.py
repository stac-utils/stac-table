"""
Generates a STAC item for GBIF, using the parquet files on Azure: http://aka.ms/ai4edata-gbif.

Requires `adlfs` and `lxml` in addition to the other packages.
"""
import json
import stac_table
import datetime
import pystac
import pandas as pd


def main():
    url = "abfs://gbif/occurrence/2021-08-01/occurrence.parquet/"
    storage_options = {"account_name": "ai4edataeuwest"}
    geo_arrow_metadata = {
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "crs": None,
                "encoding": "WKB",
                "bbox": [-179.966667, -84.5833, 179.192, 78.95],
            }
        },
        "schema_version": "0.1.0",
        "creator": {"library": "geopandas", "version": "0.9.0"},
    }
    item = pystac.Item(
        "gbif-2021-08-01",
        geometry=None,
        bbox=(-180, -90, 180, 90),
        datetime=datetime.datetime(2021, 8, 1),  # snapshot date seems most useful?
        properties={},
    )

    result = stac_table.generate(
        url,
        item,
        storage_options=storage_options,
        geo_arrow_metadata=geo_arrow_metadata,
    )

    # Add in the descriptions
    descriptions = (
        pd.read_html(
            "https://github.com/microsoft/AIforEarthDataSets/blob/main/data/gbif.md"
        )[0]
        .set_axis(["field", "type", "nullable", "description"], axis="columns")
        .set_index("field")["description"]
        .to_dict()
    )

    for column in result.properties["table:columns"]:
        column["description"] = descriptions[column["name"]]

    with open("item.json", "w") as f:
        json.dump(result.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()
