"""
Generates a STAC Collection and item for GBIF, using the parquet files on
Azure: http://aka.ms/ai4edata-gbif.

Requires `adlfs` and `lxml` in addition to the other packages.

The files can be uploaded with, e.g.

$ azcopy copy items 'https://ai4edataeuwest.blob.core.windows.net/gbif-stac' --recursive
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
    asset_extra_fields = {"table:storage_options": storage_options}
    p = Path("items")
    p.mkdir(exist_ok=True)

    for path in dates:
        print("processing", path)
        date = datetime.datetime(*list(map(int, path.split("/")[-1].split("-"))))
        date_id = f"{date:%Y-%m-%d}"

        item = pystac.Item(
            f"gbif-{date_id}",
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
            f"abfs://gbif/occurrence/{date_id}/occurrence.parquet",
            item,
            storage_options=storage_options,
            proj=False,
            asset_extra_fields=asset_extra_fields,
            count_rows=False,
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
        with open(p.joinpath(item.id + ".json"), "w") as f:
            json.dump(result.to_dict(), f)

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

    pystac.extensions.item_assets.ItemAssetsExtension.add_to(collection)
    collection.extra_fields["item_assets"] = {
        "data": {
            "type": stac_table.PARQUET_MEDIA_TYPE,
            "title": "Dataset root",
            "roles": ["data"],
            **asset_extra_fields,
        }
    }

    collection.stac_extensions.append(stac_table.SCHEMA_URI)
    collection.keywords = ["GBIF", "Biodiversity", "Species"]
    collection.extra_fields["msft:short_description"] = (
        "An international network and data infrastructure funded by the world's "
        "governments providing global data that document the occurrence of species."
    )
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
        href=(
            "https://ai4edatasetspublicassets.blob.core.windows.net/"
            "assets/pc_thumbnails/gbif.png"
        ),
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
