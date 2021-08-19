"""
Generates a STAC Collection and item for GBIF, using the parquet files on Azure: http://aka.ms/ai4edata-gbif.

Requires `adlfs` and `lxml` in addition to the other packages.
"""
import json
import stac_table
import datetime
import pystac
import pandas as pd


def main():
    url = "abfs://gbif/occurrence/2021-08-01/occurrence.parquet"
    storage_options = {"account_name": "ai4edataeuwest"}
    item = pystac.Item(
        "gbif-2021-08-01",
        geometry=None,
        bbox=(-180, -90, 180, 90),
        datetime=datetime.datetime(2021, 8, 1),  # snapshot date seems most useful?
        properties={},
    )

    result = stac_table.generate(url, item, storage_options=storage_options, proj=False)

    # Add in the descriptions
    descriptions = (
        pd.read_html(
            "https://github.com/microsoft/AIforEarthDataSets/blob/main/data/gbif.md"
        )[0]
        .set_axis(["field", "type", "nullable", "description"], axis="columns")
        .set_index("field")["description"]
        .to_dict()
    )

    # Add in the storage options
    # TODO: move this from xarray-assets to fsspec-assets. Store it on the asset?

    for column in result.properties["table:columns"]:
        column["description"] = descriptions[column["name"]]

    with open("item.json", "w") as f:
        json.dump(result.to_dict(), f, indent=2)

    collection = pystac.Collection(
        "gbif",
        description=(
            "The [Global Biodiversity Information Facility](https://www.gbif.org/) (GBIF) is an international "
            "network and data infrastructure funded by the world's governments providing global data that "
            "document the occurrence of species."
        ),
        extent=pystac.Extent(
            spatial=pystac.collection.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.collection.TemporalExtent(
                [[datetime.datetime(2021, 8, 1), None]]
            ),
        ),
    )
    collection.extra_fields["table:columns"] = result.properties["table:columns"]

    with open("collection.json", "w") as f:
        json.dump(collection.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()
