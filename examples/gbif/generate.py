"""
Generates a STAC Collection and item for GBIF, using the parquet files on Azure: http://aka.ms/ai4edata-gbif.

Requires `adlfs` and `lxml` in addition to the other packages.
"""
import re
import json
import stac_table
import datetime
import pystac
from pathlib import Path
import adlfs


def main():
    fs = adlfs.AzureBlobFileSystem("ai4edataeuwest")
    dates = fs.ls("gbif/occurrence")
    storage_options = {"account_name": "ai4edataeuwest"}
    items = []

    for path in dates:
        date = datetime.datetime(*list(map(int, path.split("/")[-1].split("-"))))

        item = pystac.Item(
            "gbif-2021-08-01",
            geometry={
                "type": "Polygon",
                "coordinates": [
                    [
                        [180.0, -90.0],
                        [180.0, 90.0],
                        [-180.0, 90.0],
                        [-180.0, -90.0],
                        [180.0, -90.0],
                    ]
                ],
            },
            bbox=[-180, -90, 180, 90],
            datetime=date,  # snapshot date seems most useful?
            properties={},
        )

        result = stac_table.generate(
            f"abfs://gbif/occurrence/{date:%Y-%m-%d}/occurrence.parquet",
            item,
            storage_options=storage_options,
            proj=False,
            asset_extra_fields=storage_options,
        )
        xpr = re.compile(
            r"^\|\s*(\w*?)\s*\| \w.*?\|.*?\|\s*(.*?)\s*\|$", re.UNICODE | re.MULTILINE
        )
        column_descriptions = dict(
            xpr.findall(Path("column_descriptions.md").read_text())
        )

        # Add in the storage options
        # TODO: move this from xarray-assets to fsspec-assets. Store it on the asset?

        for column in result.properties["table:columns"]:
            column["description"] = column_descriptions[column["name"]]

        result.validate()
        items.append(result)

    with open("items.ndjson", "w") as f:
        for item in items:
            json.dump(item.to_dict(), f)
            f.write("\n")

    with open("item.json", "w") as f:
        json.dump(items[-1].to_dict(), f, indent=2)

    dates = [
        datetime.datetime(*list(map(int, dates[0].split("/")[-1].split("-")))),
        datetime.datetime(*list(map(int, dates[-1].split("/")[-1].split("-")))),
    ]
    collection = pystac.Collection(
        "gbif",
        description="{{ collection.description }}",
        extent=pystac.Extent(
            spatial=pystac.collection.SpatialExtent([[-180, -90, 180, 90]]),
            temporal=pystac.collection.TemporalExtent([dates]),
        ),
    )
    collection.extra_fields["table:columns"] = result.properties["table:columns"]
    collection.title = "Global Biodiversity Information Facility (GBIF)"
    # TODO: Add table extension.
    # Blocked until we have this published officially.
    # collection.stac_extensions.append(
    #     "https://stac-extensions.github.io/table/v1.0.0/schema.json"
    # )
    collection.keywords = ["GBIF", "Biodiversity", "Species"]
    collection.extra_fields[
        "msft:short_description"
    ] = "An international network and data infrastructure funded by the world's governments providing global data that document the occurrence of species."
    collection.extra_fields["msft:container"] = "gbif"
    collection.extra_fields["msft:storage_account"] = "ai4edataeuwest"
    collection.providers = [
        pystac.Provider(
            "Global Biodiversity Information Facility",
            roles=[
                pystac.provider.ProviderRole.PRODUCER,
                pystac.provider.ProviderRole.LICENSOR,
                pystac.provider.ProviderRole.PROCESSOR,
            ],
            url="https://www.gbif.org/",
        ),
        pystac.Provider(
            "Microsoft",
            roles=[pystac.provider.ProviderRole.HOST],
            url="https://planetarycomputer.microsoft.com",
        ),
    ]
    collection.assets["thumbnail"] = pystac.Asset(
        title="Forest Inventory and Analysis",
        href="https://camo.githubusercontent.com/40dc57a0d4f7a365af940e1ce43b419dc12b173eea645907c28e2a2a50267324/68747470733a2f2f6c6162732e676269662e6f72672f7e6d626c6973736574742f323031392f31302f616e616c79746963732d6d6170732f776f726c642d323032312d30312d30312e706e67",
        media_type="image/png",
    )
    collection.links = [
        pystac.Link(
            pystac.RelType.LICENSE,
            target="https://www.gbif.org/terms",
            media_type="text/html",
            title="Terms of use",
        )
    ]
    collection.validate()

    with open("collection.json", "w") as f:
        json.dump(collection.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()
