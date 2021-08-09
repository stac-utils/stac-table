# stac-tables

This library generates STAC objects from tabular dataset. It uses the `table` extension.

## Examples

Generate a STAC item from a Parquet Dataset

```python
>>> import geopandas, pyarrow.parquet, pystac, stac_table
>>> # generate the sample data
>>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
>>> gdf.to_parquet("data.parquet")
>>> # Create the template Item
>>> item = pystac.Item(
...     "naturalearth_lowres", geometry=None, bbox=None, datetime="2021-01-01", properties={}
... )
>>> ds = pyarrow.parquet.ParquetDataset("data.parquet", use_legacy_dataset=False)
>>> result = stac_table.generate(ds, item)
>>> result
<Item id=naturalearth_lowres>
```