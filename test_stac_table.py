import datetime
import os
import shutil
from pathlib import Path

import pandas as pd
import dask_geopandas
import geopandas
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

        item = pystac.Item(
            "naturalearth_lowres", geometry, bbox, datetime.datetime(2021, 1, 1), {}
        )
        ds = str(Path("data.parquet").absolute())
        result = stac_table.generate(
            ds,
            item,
            asset_extra_fields={"table:storage_options": {"storage_account": "foo"}},
        )

        expected_columns = [
            {"name": "pop_est", "type": "double"},
            {"name": "continent", "type": "string"},
            {"name": "name", "type": "string"},
            {"name": "iso_a3", "type": "string"},
            {"name": "gdp_md_est", "type": "int64"},
            {"name": "geometry", "type": "byte_array"},
        ]
        assert result.properties["table:columns"] == expected_columns

        asset = result.assets["data"]
        assert asset.href == ds
        assert asset.media_type == "application/x-parquet"
        assert asset.roles == ["data"]
        assert asset.extra_fields["table:storage_options"] == {"storage_account": "foo"}

        assert pystac.extensions.projection.SCHEMA_URI in result.stac_extensions
        assert result.properties["proj:epsg"] == 4326

    def test_infer_bbox(self):
        df = geopandas.GeoDataFrame(
            {"A": [1, 2]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
            crs=4326,
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate("data.parquet", item, infer_bbox=True)

        assert result.bbox == (1.0, 2.0, 2.0, 3.0)
        assert result.properties["proj:epsg"] == 4326
        assert result.properties["proj:bbox"] == (1.0, 2.0, 2.0, 3.0)

    def test_infer_geometry(self):
        df = geopandas.GeoDataFrame(
            {"A": [1, 2]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
            crs=4326,  # TODO: other epsg
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate("data.parquet", item, infer_geometry=True)
        expected = {"type": "MultiPoint", "coordinates": ((1.0, 2.0), (2.0, 3.0))}

        assert result.geometry == expected
        assert result.properties["proj:epsg"] == 4326
        assert result.properties["proj:geometry"] == expected

    def test_infer_datetime_midpoint(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="midpoint"
        )
        assert result.properties["datetime"] == "2021-01-01T00:00:00Z"

    def test_infer_datetime_unique(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-01")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="unique"
        )
        assert result.properties["datetime"] == "2021-01-01T00:00:00Z"

    def test_infer_datetime_range(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="range"
        )

        assert result.properties["start_datetime"] == "2000-01-01T00:00:00Z"
        assert result.properties["end_datetime"] == "2000-01-03T00:00:00Z"

    def test_metadata(self):
        df = geopandas.GeoDataFrame(
            {"A": [pd.Timestamp("2000-01-01"), pd.Timestamp("2000-01-03")]},
            geometry=[shapely.geometry.Point(1, 2), shapely.geometry.Point(2, 3)],
        )
        df.to_parquet("data.parquet")
        item = pystac.Item(
            "naturalearth_lowres", None, None, datetime.datetime(2021, 1, 1), {}
        )
        result = stac_table.generate(
            "data.parquet", item, datetime_column="A", infer_datetime="range"
        )
        assert "metadata" not in result.properties["table:columns"][0]

        # Bug in pandas? We apparently aren't writing the metadata...
        # df["A"].attrs = {"key": "value!"}
        # df.to_parquet("data.parquet")
        # item = pystac.Item("naturalearth_lowres", None, None,
        #                    datetime.datetime(2021, 1, 1), {})
        # result = stac_table.generate(
        #     "data.parquet", item, datetime_column="A", infer_datetime="range"
        # )
        # assert result.properties["table:columns"][0]["metadata"] == {"key": "value!"}
