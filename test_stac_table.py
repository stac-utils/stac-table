import os
import shutil
import warnings
from pathlib import Path

import dask_geopandas
import geopandas
import pyarrow.parquet
import pystac
import pytest
import shapely.geometry
import stac_table


@pytest.fixture
def ensure_clean():
    yield
    shutil.rmtree("data.parquet", ignore_errors=True)
    try:
        os.remove("data.parquet")
    except FileNotFoundError:
        pass


@pytest.mark.usefixtures("ensure_clean")
class TestItem:
    @pytest.mark.parametrize("partition", [True, False])
    def test_generate_item(self, partition):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="An exception was ignored")
            gdf = geopandas.read_file(
                geopandas.datasets.get_path("naturalearth_lowres")
            )

        union = gdf["geometry"].unary_union
        geometry = shapely.geometry.mapping(union)
        bbox = union.bounds

        if partition:
            gdf = dask_geopandas.from_geopandas(gdf, npartitions=2)

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", message=".*initial implementation of Parquet.*"
            )
            gdf.to_parquet("data.parquet")

        ds = pyarrow.parquet.ParquetDataset("data.parquet", use_legacy_dataset=False)

        item = pystac.Item("naturalearth_lowres", geometry, bbox, "2021-01-01", {})
        result = stac_table.generate(ds, item)

        expected_columns = [
            {"name": "pop_est", "metadata": None},
            {"name": "continent", "metadata": None},
            {"name": "name", "metadata": None},
            {"name": "iso_a3", "metadata": None},
            {"name": "gdp_md_est", "metadata": None},
            {"name": "geometry", "metadata": None},
        ]
        if partition:
            expected_columns.append({"name": "__null_dask_index__", "metadata": None})
        assert result.properties["table:columns"] == expected_columns

        expected_geo_arrow_metadata = {
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

        if partition:
            # bug in dask-geopandas https://github.com/geopandas/dask-geopandas/issues/94
            expected_geo_arrow_metadata["columns"]["geometry"]["bbox"] = [
                -180.0,
                -55.61183,
                180.00000000000006,
                83.64513000000001,
            ]

        assert (
            result.properties["table:geo_arrow_metadata"] == expected_geo_arrow_metadata
        )

        # From a path
        result = stac_table.generate(str(Path("data.parquet").absolute()), item)
        assert result.properties["table:columns"] == expected_columns
        assert (
            result.properties["table:geo_arrow_metadata"] == expected_geo_arrow_metadata
        )
