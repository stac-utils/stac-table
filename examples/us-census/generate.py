import json
import os
import re
import datetime
import itertools
from pathlib import Path

import adlfs
import pystac
import stac_table
import shapely.geometry
import rich
import dask_geopandas


AZURE_SAS_TOKEN = os.environ["AZURE_SAS_TOKEN"]
table_descriptions = {
    "cb_2020_us_aiannh_500k": (
        "American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH)",
        "This file contains data for legal and statistical [American Indian/Alaska Native Areas/Hawaiian Home Lands (AIANNH)](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_1) entities published by the US Census Bureau.",
        slice(0, 9),
    ),
    "cb_2020_us_aitsn_500k": (
        "American Indian Tribal Subdivisions (AITSN)",
        "This file contains data on [American Indian Tribal Subdivisions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_1). These areas are the legally defined subdivisions of American Indian Reservations (AIR), Oklahoma Tribal Statistical Areas (OTSA), and Off-Reservation Trust Land (ORTL).",
        slice(9, 20),
    ),
    "cb_2020_02_anrc_500k": (
        "Alaska Native Regional Corporations (ANRC)",
        "This file contains data on [Alaska Native Regional Corporations](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_1), which are corporations created according to the Alaska Native Claims Settlement Act. ",
        slice(20, 30),
    ),
    "cb_2020_us_tbg_500k": (
        "Tribal Block Groups (TBG)",
        "This file includes data on [Tribal Block Groups](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_26), which are subdivisions of Tribal Census Tracts. These block groups can extend over multiple AIRs and ORTLs due to areas not meeting Block Group minimum population thresholds.",
        slice(30, 40),
    ),
    "cb_2020_us_ttract_500k": (
        "Tribal Census Tracts (TTRACT)",
        "This file includes data on [Tribal Census Tracts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_27) which are relatively small statistical subdivisions of AIRs and ORTLs defined by federally recognized tribal government officials in partnership with the Census Bureau. Due to population thresholds, the Tracts may consist of multiple non-contiguous areas.",
        slice(40, 50),
    ),
    "cb_2020_us_bg_500k": (
        "Census Block Groups (BG)",
        "This file contains data on [Census Block Groups](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_4). These groups are the second smallest geographic grouping. They consist of clusters of blocks within the same census tract that share the same first digit of their 4-character census block number. Census Block Groups generally contain between 600 and 3,000 people and generally cover contiguous areas.",
        slice(50, 62),
    ),
    "cb_2020_us_tract_500k": (
        "Census Tracts (TRACT)",
        "This file contains data on [Census Tracts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_13) which are small and relatively permanent statistical subdivisions of a county or equivalent entity. Tract population size is generally between 1,200 and 8,000 people with an ideal size of 4,000. Boundaries tend to follow visible and identifiable features and are usually contiguous areas.",
        slice(62, 76),
    ),
    "cb_2020_us_cd116_500k": (
        "Congressional Districts: 116th Congress (CD116)",
        "This file contains data on the [Congressional Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_9) for the 116th Congress. ",
        slice(76, 86),
    ),
    "cb_2020_us_concity_500k": (
        "Consolidated Cities (CONCITY)",
        "This file contains data on [Consolidated Cities](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_8). These are areas where one or several other incorporated places in a county or Minor Civil Division are included in a consolidated government but still exist as separate legal entities.",
        slice(86, 97),
    ),
    "cb_2020_us_county_500k": (
        "Counties (COUNTY)",
        'This file contains data on [Counties and Equivalent Entities](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_12). These are the primary legal divisions of states. Most states use the term "counties," but other terms such as "Parishes," "Municipios," or "Independent Cities" may be used. ',
        slice(97, 109),
    ),
    "cb_2020_us_county_within_cd116_500k": (
        "Counties within Congressional Districts: 116th Congress (COUNTY_within_CD116)",
        "This file contains data on Counties within Congressional Districts.",
        slice(111, 119),  # Skip PARTFLG
    ),
    "cb_2020_us_cousub_500k": (
        "County Subdivisions (COUSUB)",
        "This file contains [County Subdivisions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_11), which are the primary divisions of counties and equivalent entities. These divisions vary from state to state and include Barrios, Purchases, Townships, and other types of legal and statistical entities. ",
        slice(119, 134),
    ),
    "cb_2020_us_division_500k": (
        "Divisions (DIVISION)",
        "This file contains data on [Divisions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_10) of the US. This file is similar to the Regions file but contains more divisions and encompasses several states per division.",
        slice(134, 143),
    ),
    "cb_2020_us_cbsa_500k": (
        "Core Based Statistical Areas (CBSAs)",
        "This file contains data on [Core Based Statistical Areas (CBSAs)](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7). This encompasses all metropolitan and micropolitan statistical areas.",
        slice(143, 153),
    ),
    "cb_2020_us_csa_500k": (
        "Combined Statistical Areas (CSA)",
        "This file contains data on [Combined Statistical Areas](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7), which are areas that consist of two or more adjacent CBSAs that have significant employment interchanges.",
        slice(153, 162),
    ),
    "cb_2020_us_metdiv_500k": (
        "Metropolitan Divisions (METDIV)",
        "This file contains data on [Metropolitan Divisions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7). These areas are groupings of counties or equivalent entities within a metropolitan statistical area with a core of 2.5 million inhabitants and one or more main counties that represent employment centers, plus adjacent counties with commuting ties.",
        slice(162, 173),
    ),
    "cb_2020_us_necta_500k": (
        "New England City and Town Areas (NECTA)",
        "This file contains [New England City and Town Areas](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7), which encompass metropolitan and micropolitan statistical areas and urban clusters in New England.",
        slice(173, 183),
    ),
    "cb_2020_us_nectadiv_500k": (
        "New England City and Town Area Division (NECTADIV)",
        "This file contains [New England City and Town Areas Divisions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7), which are smaller groupings of cities and towns in New England that contain a single core of 2.5 million inhabitants. Each division must have a total population of 100,000 or more.",
        slice(183, 194),
    ),
    "cb_2020_us_cnecta_500k": (
        "Combined New England City and Town Areas (CNECTA)",
        "This file contains data on [Combined New England City and Town Areas](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_7), consisting of two or more adjacent NECTAs that have significant employment interchanges.",
        slice(194, 203),
    ),
    "cb_2020_us_place_500k": (
        "Places (PLACE)",
        "This file contains [Places](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_14) which are Incorporated Places (legal entities) and Census Designated Places (CDPs, statistical entities). An incorporated place usually is a city, town, village, or borough but can have other legal descriptions. CDPs are settled concentrations of population that are identifiable by name but are not legally incorporated.",
        slice(203, 216),
    ),
    "cb_2020_us_region_500k": (
        "Regions (REGION)",
        "This file contains [Regions](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_10) of the US and encompasses several states per division.",
        slice(216, 225),
    ),
    "cb_2020_us_elsd_500k": (
        "School Districts - Elementary (ELSD)",
        "This file contains [Elementary School Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_23), referring to districts with elementary schools.",
        slice(225, 236),
    ),
    "cb_2020_us_scsd_500k": (
        "School Districts - Secondary (SCSD)",
        "This file contains [Secondary School Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_23), referring to districts with secondary schools.",
        slice(236, 246),
    ),
    "cb_2020_us_unsd_500k": (
        "School Districts - Unified (UNSD)",
        "This file contains [Unified School Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_23), referring to districts that provide education to children of all school ages. Unified school districts can have both secondary and elementary schools.",
        slice(246, 257),
    ),
    "cb_2020_us_sldl_500k": (
        "State Legislative Districts - Lower Chamber (SLDL)",
        "This file contains [Lower Chamber State Legislative Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_24).",
        slice(257, 270),
    ),
    "cb_2020_us_sldu_500k": (
        "State Legislative Districts - Upper Chamber (SLDU)",
        "This file contains [Upper Chamber State Legislative Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_24).",
        slice(270, 283),
    ),
    "cb_2020_us_state_500k": (
        "States (STATE)",
        "This file contains the [US States and State Equivalent Entities](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_25). Within Census Bureau datasets, the District of Columbia, Puerto Rico, and the Island Areas (American Samoa, the Commonwealth of the Northern Mariana Islands, Guam, and the US Virgin Islands) are treated as statistical equivalents of states alongside the 50 US states.",
        slice(283, 293),
    ),
    "cb_2020_72_subbarrio_500k": (
        "Subbarrios (SUBBARRIO)",
        'This file contains [Subbarrios](https://www.census.gov/programs-surveys/geography/about/glossary.html#pr), which are legally defined subdivisions of Minor Civil Division in Puerto Rico. They don"t exist within every Minor Civil Division and don"t always cover the entire Minor Civil Division where they do exist.',
        slice(293, 306),
    ),
    "cb_2020_us_nation_5m": (
        "United States Outline",
        "This file contains the [United States Outline](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_30) shapefile. This contains all 50 US states plus the District of Columbia, Puerto Rico, and the Island Areas (American Samoa, the Commonwealth of the Northern Mariana Islands, Guam, and the US Virgin Islands). There is only one feature within this dataset.",
        None,
    ),
    "cb_2020_us_vtd_500k": (
        "Voting Districts (VTD)",
        "This file contains all [US Voting Districts](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_31), which are geographic features established by state, local and tribal governments to conduct elections.",
        slice(306, None),
    ),
}


def main():
    fs = adlfs.AzureBlobFileSystem("ai4edataeuwest", credential=AZURE_SAS_TOKEN)
    datasets = fs.ls("us-census/2020")
    storage_options = {"account_name": "ai4edataeuwest", "credential": AZURE_SAS_TOKEN}
    asset_extra_fields = {"table:storage_options": {"account_name": "ai4edataeuwest"}}

    p = Path("items")
    p.mkdir(exist_ok=True)
    doc = json.loads(open("census-data.ipynb").read())
    xpr = re.compile(". (\S+) = (.*)")
    markdown_cells = list(
        itertools.chain(
            *[x["source"] for x in doc["cells"] if x["cell_type"] == "markdown"]
        )
    )
    names_descriptions = [xpr.match(x).groups() for x in markdown_cells if xpr.match(x)]

    for dataset in datasets:
        if Path(dataset).stem not in table_descriptions:
            rich.print(["[red]Skipping[/red]", dataset])
            continue
        column_slice = table_descriptions[Path(dataset).stem][2]
        if column_slice:
            column_descriptions = dict(names_descriptions[column_slice])
        else:
            column_descriptions = {}

        outfile = p.joinpath(Path(dataset).name).with_suffix(".json")
        df = dask_geopandas.read_parquet(
            f"abfs://{dataset}", storage_options=storage_options
        )
        geom = df.spatial_partitions.unary_union

        item = pystac.Item(
            f"2020-{outfile.stem}",
            geometry=shapely.geometry.mapping(geom),
            bbox=geom.bounds,
            datetime=datetime.datetime(2021, 8, 1),
            properties={},
        )
        result = stac_table.generate(
            f"abfs://{dataset}",
            item,
            storage_options=storage_options,
            asset_extra_fields=asset_extra_fields,
        )
        for column in result.properties["table:columns"]:
            if column["name"] in column_descriptions:
                column["description"] = column_descriptions[column["name"]]

        with open(outfile, "w") as f:
            json.dump(result.to_dict(), f)

        rich.print(f"[green]Processed[/green] {dataset}")

    collection = pystac.Collection(
        "us-census",
        description="{{ collection.description }}",
        extent=pystac.Extent(
            # spatial extent from the US Boundary file's bounds.
            spatial=pystac.collection.SpatialExtent(
                [[-179.14733999999999, -14.552548999999999, 179.77847, 71.352561]]
            ),
            temporal=pystac.collection.TemporalExtent(
                [datetime.datetime(2021, 8, 1), datetime.datetime(2021, 8, 1)]
            ),
        ),
    )
    table_tables = [
        {"name": name, "description": description, "msft:item_name": k}
        for k, (name, description, _) in table_descriptions.items()
    ]

    collection.title = "US Census"
    # TODO: Add table extension.
    # Blocked until we have this published officially.
    # collection.stac_extensions.append(
    #     "https://stac-extensions.github.io/table/v1.0.0/schema.json"
    # )
    collection.keywords = ["US Census Bureau", "Administrative boundaries"]
    collection.extra_fields["table:tables"] = table_tables
    collection.extra_fields[
        "msft:short_description"
    ] = "US population counts by various geographic boundaries."
    collection.extra_fields["msft:container"] = "us-census"
    collection.extra_fields["msft:storage_account"] = "ai4edataeuwest"
    collection.providers = [
        pystac.Provider(
            "United States Census Bureau",
            roles=[
                pystac.provider.ProviderRole.PRODUCER,
                pystac.provider.ProviderRole.LICENSOR,
                pystac.provider.ProviderRole.PROCESSOR,
            ],
            url="https://www.census.gov/en.html",
        ),
        pystac.Provider(
            "makepath",
            roles=[pystac.provider.ProviderRole.PROCESSOR],
            url="https://makepath.com/",
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
            target="https://www.census.gov/about/policies/open-gov/open-data.html",
            media_type="text/html",
            title="Terms of use",
        )
    ]
    collection.validate()

    with open("collection.json", "w") as f:
        json.dump(collection.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()
