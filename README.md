# stac-table

This library generates STAC objects for tabular dataset. It uses the [`table`][table] STAC extension.

## Examples

Generate a STAC item from a Parquet Dataset.

```python
>>> import datetime, geopandas, pyarrow.parquet, pystac, stac_table
>>> # generate the sample data
>>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
>>> gdf.to_parquet("data.parquet")
>>> # Create the template Item
>>> item = pystac.Item(
...     "naturalearth_lowres", geometry=None, bbox=None, datetime=datetime.datetime(2021, 1, 1), properties={}
... )
>>> ds = pyarrow.parquet.ParquetDataset("data.parquet", use_legacy_dataset=False)
>>> result = stac_table.generate(ds, item)
>>> result
<Item id=naturalearth_lowres>
```

The new item is updated to include the `table` STAC extension

```python
>>> result.stac_extensions
['https://stac-extensions.github.io/table/v1.0.0/schema.json']
```

The updated fields are available under `properties`.

```python
>>> result.properties["table:columns"]
[{'name': 'pop_est', 'metadata': None},
 {'name': 'continent', 'metadata': None},
 {'name': 'name', 'metadata': None},
 {'name': 'iso_a3', 'metadata': None},
 {'name': 'gdp_md_est', 'metadata': None},
 {'name': 'geometry', 'metadata': None}]

>>> result.properties["table:geo_arrow_metadata"]
{'primary_column': 'geometry',
 'columns': {'geometry': {'crs': 'GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]',
   'encoding': 'WKB',
   'bbox': [-180.0, -90.0, 180.00000000000006, 83.64513000000001]}},
 'schema_version': '0.1.0',
 'creator': {'library': 'geopandas', 'version': '0.9.0'}}
```

Finally, an Asset is added with a link to the  the dataset,

```python
>>> result.assets["data"].to_dict()
{'href': 'data.parquet',
 'type': 'application/x-parquet',
 'title': 'Dataset root',
 'roles': ['data']}
```

[table]: https://github.com/TomAugspurger/table