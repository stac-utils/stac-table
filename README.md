# stac-table

This library generates STAC objects for tabular dataset. It uses the [`table`][table] STAC extension.

## Installation

`stac-table` is only available through GitHub right now:

```
python -m pip install git+https://github.com/TomAugspurger/stac-table
```

## Examples

Generate a STAC item from a Parquet Dataset.

```python
>>> import datetime, geopandas, pystac, stac_table
>>> # generate the sample data
>>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
>>> gdf.to_parquet("data.parquet")
>>> # Create the template Item
>>> item = pystac.Item(
...     "naturalearth_lowres", geometry=None, bbox=None, datetime=datetime.datetime(2021, 1, 1), properties={}
... )
>>> result = stac_table.generate("data.parquet", item)
>>> result
<Item id=naturalearth_lowres>
```

The new item is updated to include the `table` STAC extension

```python
>>> result.stac_extensions
['https://stac-extensions.github.io/table/v1.0.0/schema.json',
 'https://stac-extensions.github.io/projection/v1.0.0/schema.json']
```

The updated fields are available under `properties`.

```python
>>> result.properties
{'table:columns': [{'name': 'pop_est', 'type': 'int64'},
  {'name': 'continent', 'type': 'byte_array'},
  {'name': 'name', 'type': 'byte_array'},
  {'name': 'iso_a3', 'type': 'byte_array'},
  {'name': 'gdp_md_est', 'type': 'double'},
  {'name': 'geometry', 'type': 'byte_array'}],
 'proj:epsg': 4326}
```

Finally, an Asset is added with a link to the  the dataset,

```python
>>> result.assets["data"].to_dict()
{'href': 'data.parquet',
 'type': 'application/x-parquet',
 'title': 'Dataset root',
 'roles': ['data']}
```

`stac_table` will optionally fill in some additional values in your STAC item if you pass the appropriate keywords.

* `infer_bbox`: Sets the item's `bbox` to the bounding box of the union of the geometry column's values. Relies on spatial partitions.
* `infer_geometry`: Sets the item's `geometry` to the union of the geometry column's values.
* `infer_datetime`: Sets the item's `properties.datetime` or `properties.start_datetime` and `properties.end_daetime` based on the values in the `datetime_column` column.

[table]: https://github.com/TomAugspurger/table
