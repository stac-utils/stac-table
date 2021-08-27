import json
import stac_table
import datetime
import pystac
import dask.dataframe as dd
import pandas as pd
import urllib.request
from pathlib import Path
import tabula


# See the table of contents in the user guide.
table_subsections = [
    # Location
    ("survey", "2.1"),
    ("county", "2.3"),
    ("plot", "2.4"),
    ("cond", "2.5"),
    ("subplot", "2.6"),
    ("subp_cond", "2.7"),
    ("boundary", "2.8"),  # This is just documented elsewhere :(
    (
        "subp_cond_chng_mtrx",
        "2.8",
    ),  # this is actually 2.9, but there's a bug in the so that the table subsection is 2.8
    # Tree
    ("tree", "3.1"),
    ("tree_woodland_stems", "3.2"),
    ("tree_regional_biomass", "3.3"),
    ("tree_grm_component", "3.4"),
    # missing threshold
    ("tree_grm_midpt", "3.6"),
    ("tree_grm_begin", "3.7"),
    ("tree_grm_estn", "3.8"),
    # missing beginend
    ("seedling", "3.10"),
    ("sitetree", "3.11"),
    # Invasive species; Understory vegetation; Ground cover
    ("invasive_subplot_spp", "4.1"),
    ("p2veg_subplot_spp", "4.2"),
    ("p2veg_subp_structure", "4.3"),
    # ("grnd_cvr", "4.4"),  # this is empty
    # Down woody material
    ("dwm_visit", "5.1"),
    ("dwm_coarse_woody_debris", "5.2"),
    ("dwm_duff_litter_fuel", "5.3"),
    ("dwm_fine_woody_debris", "5.4"),
    ("dwm_microplot_fuel", "5.5"),
    ("dwm_residual_pile", "5.6"),
    ("dwm_transect_segment", "5.7"),
    ("cond_dwm_calc", "5.8"),
    # Northern Research Station Tree regeneration indicator
    ("plot_regen", "6.1"),
    ("subplot_regen", "6.2"),
    ("seedling_regen", "6.3"),
    # Population
    ("pop_estn_unit", "7.1"),
    ("pop_eval", "7.2"),
    ("pop_eval_attribute", "7.3"),
    ("pop_eval_grp", "7.4"),
    ("pop_eval_typ", "7.5"),
    ("pop_plot_stratum_assgn", "7.6"),
    ("pop_stratum", "7.7"),
    # Plot geometry; plot snapshot
    ("plotgeom", "8.1"),
    ("plotsnap", "8.2"),
]

# These tables do not appear in the database guide.
#     # ("lichen_lab",
#     # ("lichen_plot_summary",
#     # ("lichen_visit",
#     # ("ozone_biosite_summary",
#     # ("ozone_plot",
#     # ("ozone_plot_summary",
#     # ("ozone_species_summary",
#     # ("ozone_validation",
#     # ("ozone_visit",
#     # ("soils_erosion",
#     # ("soils_lab",
#     # ("soils_sample_loc",
#     # ("soils_visit",
#     # ("tree_grm_threshold",
#     # ("veg_plot_species",
#     # ("veg_quadrat",
#     # ("veg_subplot",
#     # ("veg_subplot_spp",
#     # ("veg_visit",

# The documentation for boundary is broken. We'll do it here.
boundary_column_descriptions = {
    "CN": "Sequence number",
    "PLT_CN": "Plot sequence number",
    "INVYR": "Inventory year",
    "STATECD": "State cod ",
    "UNITCD": "Survey unit code",
    "COUNTYCD": "County code",
    "PLOT": "Plot number",
    "SUBP": "Subplot number",
    "SUBPTYP": "Plot type code",
    "BNDCHG": "Boundary change code",
    "CONTRAST": "Contrasting condition",
    "AZMLEFT": "Left azimuth",
    "AZMCORN": "Corner azimuth",
    "DISTCORN": "Corner distance",
    "AZMRIGHT": "Right azimuth",
    "CYCLE": "Inventory cycle number",
    "SUBCYCLE": "Inventory subcycle number",
    "CREATED_BY": "Created by",
    "CREATED_DATE": "Created date",
    "CREATED_IN_INSTANCE": "Created in instance",
    "MODIFIED_BY": "Modified by",
    "MODIFIED_DATE": "Modified date",
    "MODIFIED_IN_INSTANCE": "Modified in instance",
}
boundary_table_description = "Boundary table. This table provides a description of the demarcation line between two conditions that occur on a single subplot"

additional_table_descriptions = {
    "subp_cond_chng_mtrx": """\
    Subplot condition change matrix table. This table contains information about the
    mix of current and  previous conditions that occupy the same area on the subplot.

    * PLOT.CN = SUBP_COND_CHNG_MTRX.PLT_CN links the subplot condition change matrix records to the unique plot record.
    * PLOT.PREV_PLT_CN = SUBP_COND_CHNG_MTRX.PREV_PLT_CN links the subplot condition change matrix records to the unique previous plot record
    """
}


def main():
    guide = Path("FIADB User Guide P2_9-0_final.pdf")
    if not guide.exists():
        urllib.request.urlretrieve(
            "https://www.fia.fs.fed.us/library/database-documentation/current/ver90/FIADB%20User%20Guide%20P2_9-0_final.pdf",
            guide,
        )

    print("parsing PDF. This will take a minute...")
    parsed = tabula.read_pdf(
        guide.name, pages="all", lattice=True, pandas_options=dict(dtype=object)
    )
    # The documentation uses two headers for table descriptions. Handle both.
    column_sets = [
        [
            "Subsection",
            "Column name (attribute)",
            "Descriptive name",
            "Oracle data type",
        ],
        ["Subsection", "Column name", "Descriptive name", "Oracle data type"],
    ]
    column_descriptions = [
        x
        for x in parsed
        if any(list(x.columns) == column_set for column_set in column_sets)
    ]
    storage_options = {"account_name": "cpdataeuwest"}
    items = Path("items")
    items.mkdir(exist_ok=True)

    for table_name, subsection in table_subsections:
        # if Path(f"{table_name}.json").exists():
        #     continue
        column_description = (
            pd.concat(
                [
                    x
                    for x in column_descriptions
                    if x["Subsection"].str.startswith(subsection + ".").all()
                ]
            )
            .iloc[:, [1, 2]]
            .dropna()
        )
        if table_name == "boundary":
            d = boundary_column_descriptions
        else:
            d = (
                column_description.set_index(column_description.iloc[:, 0])[
                    "Descriptive name"
                ]
                .str.replace("\r", " ")
                .dropna()
                .rename(lambda x: x.replace("\r", ""))
                .to_dict()
            )

        uri = f"abfs://cpdata/raw/fia/{table_name}.parquet"
        df = dd.read_parquet(uri, storage_options=storage_options)

        result = list(d)
        expected = df.columns.tolist()

        if result != expected:
            print(f"Column mismatch for {table_name}!", set(result) ^ set(expected))

        item = pystac.Item(
            f"{table_name}",
            geometry=None,
            bbox=(-179.14734, -14.53, 179.77847, 71.352561),
            datetime=datetime.datetime(2020, 6, 1),  # date accessed from FIA...
            properties={},
        )
        result = stac_table.generate(
            uri,
            item,
            storage_options=storage_options,
            asset_extra_fields={"table:storage_options": storage_options},
            proj=False,
            count_rows=False,
        )
        result.properties["table:columns"] = [
            x
            for x in result.properties["table:columns"]
            if x["name"] != "__null_dask_index__"
        ]

        for column in result.properties["table:columns"]:
            description = d.get(column["name"])
            if description:
                column["description"] = d[column["name"]]
            else:
                print(f"Missing description for {table_name}.{column['name']}")

        with open(items.joinpath(f"{table_name}.json"), "w") as f:
            json.dump(result.to_dict(), f, indent=2)
        print(f"wrote {table_name}.json")

    with open("description.md", encoding="utf-8") as f:
        collection_description = f.read()

    # Handle the collection now.
    table_columns = ["Section", "Oracle table name", "Table name", "Description"]
    table_descriptions = (
        pd.concat([x for x in parsed if list(x.columns) == table_columns])
        .dropna()
        .assign(
            **{
                "Section": lambda x: x["Section"],
                "Oracle table name": lambda x: x["Oracle table name"]
                .str.lower()
                .str.replace("\r", ""),
                "Description": lambda x: x["Description"].str.replace("\r", " "),
            }
        )
        .set_index(["Section", "Oracle table name"])
    )
    names_descriptions = dict(
        zip(table_descriptions.index, table_descriptions.itertuples(index=False))
    )

    table_tables = []
    for table_name, subsection in table_subsections:
        if table_name == "subp_cond_chng_mtrx":
            # bug in docs
            subsection = "2.9"

        name, description = names_descriptions.get(
            (subsection, table_name), (None, None)
        )
        if table_name == "boundary":
            description = boundary_table_description
            name = "boundary"
        else:
            description = (
                description.replace("\n", " ").replace("\r", " ")
                if description
                else description
            )
            name = name or table_name
            name = name.replace("\r", " ")
        table_tables.append(
            {"name": name, "description": description, "msft:item_name": table_name}
        )

    collection = pystac.Collection(
        "fia",
        description="{{ collection.description }}",
        extent=pystac.Extent(
            spatial=pystac.collection.SpatialExtent(
                [
                    [-124.784, 24.744, -66.951, 49.346],  # CONUS
                    [-179.14734, 51.219862, 179.77847, 71.352561],  # Alaska
                    [-178.342102, 18.917466, -154.809379, 28.407391],  # Hawaii
                    [-171.09, -14.53, -168.16, -11.05],  # American Samoa
                    [138.06, 0.92, 163.05, 9.78],  # Federated States of Micronesia
                    [144.62, 13.24, 144.95, 13.65],  # Guam
                    [165.28, 4.57, 172.03, 14.61],  # Marshall Islands
                    [144.9, 14.11, 145.87, 20.56],  # Northern Mariana Islands
                    [131.13, 2.95, 134.73, 8.1],  # Palau
                    [-67.94, 17.92, -65.24, 18.52],  # Puerto Rico
                    [-65.04, 17.68, -64.56, 18.39],  # US Virgin Islands
                ]
            ),
            temporal=pystac.collection.TemporalExtent(
                [[datetime.datetime(2020, 6, 1), None]]
            ),
        ),
        license="CC0-1.0",
    )
    collection.title = "Forest Inventory and Analysis"
    collection.stac_extensions.append(stac_table.SCHEMA_URI)
    collection.keywords = [
        "Forest",
        "Species",
        "Carbon",
        "Biomass",
        "USDA",
        "Forest Service",
    ]
    collection.extra_fields["table:columns"] = []
    collection.extra_fields["table:tables"] = table_tables
    collection.extra_fields[
        "msft:short_description"
    ] = "Status and trends on U.S. forest location, health, growth, mortality, and production, from the US Forest Service's Forest Inventory and Analysis (FIA) program."
    collection.extra_fields["msft:container"] = "cpdata"
    collection.extra_fields["msft:storage_account"] = "cpdataeuwest"
    collection.providers = [
        pystac.Provider(
            "Forest Inventory & Analysis",
            roles=[
                pystac.provider.ProviderRole.PRODUCER,
                pystac.provider.ProviderRole.LICENSOR,
            ],
            url="https://www.fia.fs.fed.us/",
        ),
        pystac.Provider(
            "CarbonPlan",
            roles=[pystac.provider.ProviderRole.PROCESSOR],
            url="https://carbonplan.org/",
        ),
        pystac.Provider(
            "Microsoft",
            roles=[pystac.provider.ProviderRole.HOST],
            url="https://planetarycomputer.microsoft.com",
        ),
    ]
    collection.assets["thumbnail"] = pystac.Asset(
        title="Forest Inventory and Analysis",
        href="https://www.fia.fs.fed.us/library/maps/docs/USForest_fullview.gif",
        media_type="image/gif",
    )

    # https://data.nal.usda.gov/dataset/forest-inventory-and-analysis-database-0 is helpful.
    collection.links = [
        pystac.Link(
            pystac.RelType.LICENSE,
            target="https://creativecommons.org/publicdomain/zero/1.0/",
            media_type="text/html",
            title="License",
        )
    ]

    with open("collection.json", "w") as f:
        json.dump(collection.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()
