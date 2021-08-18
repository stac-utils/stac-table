import datetime
import os
import shutil
import warnings
from pathlib import Path

import pandas as pd
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
@pytest.mark.filterwarnings("ignore:An exception was ignored")
@pytest.mark.filterwarnings("ignore:.*initial implementation.*")
@pytest.mark.filterwarnings("ignore:.ParquetDataset.p.*")
class TestItem:
    @pytest.mark.parametrize("partition", [True, False])
    def test_generate_item(self, partition):
        gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))

        union = gdf["geometry"].unary_union
        geometry = shapely.geometry.mapping(union)
        bbox = union.bounds

        if partition:
            gdf = dask_geopandas.from_geopandas(gdf, npartitions=2)

        gdf.to_parquet("data.parquet")

        item = pystac.Item("naturalearth_lowres", geometry, bbox, "2021-01-01", {})
        ds = str(Path("data.parquet").absolute())
        result = stac_table.generate(ds, item)

        expected_columns = [
            {"name": "pop_est"},
            {"name": "continent"},
            {"name": "name"},
            {"name": "iso_a3"},
            {"name": "gdp_md_est"},
            {"name": "geometry"},
        ]
        if partition:
            expected_columns.append({"name": "__null_dask_index__"})
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

        asset = result.assets["data"]
        assert asset.href == ds
        assert asset.media_type == "application/x-parquet"
        if partition:
            assert asset.roles == ["data", "root"]
        else:
            assert asset.roles == ["data"]

    def test_infer_bbox(self):
        df = geopandas.GeoDataFrame(
            {"A": [1, 2]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate("data.parquet", item, infer_bbox=True)

        assert result.bbox == (1.0, 2.0, 2.0, 3.0)

    def test_infer_geometry(self):
        df = geopandas.GeoDataFrame(
            {"A": [1, 2]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate("data.parquet", item, infer_geometry=True)

        assert result.geometry == {
            "type": "MultiPoint",
            "coordinates": ((1.0, 2.0), (2.0, 3.0)),
        }

    def test_infer_datetime_midpoint(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="midpoint"
        )

        assert result.properties["datetime"] == datetime.datetime(2000, 1, 2)

    def test_infer_datetime_unique(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-01")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="unique"
        )

        assert result.properties["datetime"] == datetime.datetime(2000, 1, 1)

    def test_infer_datetime_range(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="range"
        )

        assert result.properties["start_datetime"] == datetime.datetime(2000, 1, 1)
        assert result.properties["end_datetime"] == datetime.datetime(2000, 1, 3)

    def test_metadata(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="range"
        )
        assert "metadata" not in result.properties["table:columns"][0]

        # Bug in pandas? We apparently aren't writing the metadata...
        # df["A"].attrs = {"key": "value!"}
        # df.to_parquet("data.parquet")
        # item = pystac.Item("naturalearth_lowres", None, None, "2021-01-01", {})
        # result = stac_table.generate(
        #     "data.parquet", item, datetime_column="A", infer_datetime="range"
        # )
        # assert result.properties["table:columns"][0]["metadata"] == {"key": "value!"}
