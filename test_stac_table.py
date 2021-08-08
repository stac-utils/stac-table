import warnings
import pystac
import geopandas
import stac_table
import shapely.geometry
import pyarrow.parquet

warnings.filterwarnings('ignore', message='.*initial implementation of Parquet.*')


def test_generate_collection():
    gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
    gdf.to_parquet("data.parquet")
    ds = pyarrow.parquet.ParquetDataset("data.parquet", use_legacy_dataset=False)

    geometry = shapely.geometry.mapping(gdf.geometry.unary_union)
    bbox = gdf.geometry.unary_union.bounds

    item = pystac.Item("naturalearth_lowres", geometry, bbox, "2021-01-01", {})
    result = stac_table.generate(ds, item)

    expected = [
        {"name": "pop_est", "metadata": None},
        {"name": "continent", "metadata": None},
        {"name": "name", "metadata": None},
        {"name": "iso_a3", "metadata": None},
        {"name": "gdp_md_est", "metadata": None},
        {"name": "geometry", "metadata": None},
    ]
    assert result.properties["table:columns"] == expected

    expected = {
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "crs": 'GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]',
                "encoding": "WKB",
                "bbox": [-180.0, -90.0, 180.00000000000006, 83.64513000000001],
            }
        },
        "schema_version": "0.1.0",
        "creator": {"library": "geopandas", "version": "0.9.0"},
    }
    assert result.properties["table:geo_arrow_metadata"] == expected
