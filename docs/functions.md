# DuckDB Raster Extension Function Reference

## Function Index
**[Table Functions](#table-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Drivers`](#rt_drivers) | Returns the list of supported GDAL RASTER drivers and file formats. |

----

## Table Functions

### RT_Drivers

#### Signature

```sql
RT_Drivers ()
```

#### Description

Returns the list of supported GDAL RASTER drivers and file formats.

Note that far from all of these drivers have been tested properly.
Some may require additional options to be passed to work as expected.
If you run into any issues please first consult the [consult the GDAL docs](https://gdal.org/drivers/raster/index.html).

#### Example

```sql
SELECT * FROM RT_Drivers();
```

----

