# DuckDB Raster Extension

A DuckDB extension for reading and writing geospatial raster data using SQL.

It exposes raster files (e.g. GeoTIFF, COG, VRT) as standard DuckDB tables, with one row per tile and one datacube column per band. You can filter, transform, and aggregate raster data using plain SQL, and write results back to any GDAL-supported raster format.

```sql
-- Compute NDVI from red (band 1) and NIR (band 2) directly in SQL
SELECT
	geometry,
	(databand_2 - databand_1) / (databand_2 + databand_1) AS ndvi
FROM
	RT_Read('path/to/raster/file.tif')
;
```

## How do I get it?

### Loading from community

The DuckDB **Raster Extension** is available as a signed [community extension](https://duckdb.org/community_extensions/list_of_extensions).
See more details on its [DuckDB CE web page](https://duckdb.org/community_extensions/extensions/raster.html).

To install and load it, you can run the following SQL commands in DuckDB:

```sql
INSTALL raster FROM community;
LOAD raster;
```

## Function Reference

**[Table Functions](docs/functions.md#table-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Drivers`](docs/functions.md#rt_drivers) | Returns the list of supported GDAL raster drivers and file formats. |
| [`RT_Read`](docs/functions.md#rt_read) | Reads a raster file (or a mosaic of raster files) and returns a table with the raster data. |
| [`RT_Write`](docs/functions.md#rt_write) | (`COPY TO`) Exports a data table to a new raster file. |

**[Scalar Functions](docs/functions.md#scalar-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Cube2Array`](docs/functions.md#rt_cube2array) | Extracts pixel values from a datacube column into a plain SQL array. |
| [`RT_Cube2Type`](docs/functions.md#rt_cube2type) | Changes the pixel data type of a datacube. |
| [`RT_Array2Cube`](docs/functions.md#rt_array2cube) | Packages a plain SQL array back into a datacube column. |
| [`RT_Cube<UnaryOp>`](docs/functions.md#rt_cubeunaryop) | Applies a unary operation to the datacube element-wise (`RT_CubeNeg`, `RT_CubeAbs`, …). |
| [`RT_Cube<BinaryOp>`](docs/functions.md#rt_cubebinaryop) | Applies a binary operation between two datacubes or a datacube and a scalar. Operators `+`, `-`, `*`, `/`, `^`, `%` are also supported. |
| [`RT_CubeStats`](docs/functions.md#rt_cubestats) | Calculates statistics for a specific band (0-based index) of a datacube. |
| [`RT_GdalConfig`](docs/functions.md#rt_gdalconfig) | Sets a GDAL configuration option (e.g. for S3 authentication). |

**[Spatial Functions](docs/functions.md#spatial-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Envelope`](docs/functions.md#rt_envelope) | Computes the bounding box of the valid (non-no-data) cells in the input datacube and returns it as a geometry. |
| [`RT_Polygon`](docs/functions.md#rt_polygon) | Creates a polygon geometry for each contiguous region of non-no-data values in the datacube. |
| [`RT_CubeClip`](docs/functions.md#rt_cubeclip) | Returns a datacube where cells outside the given geometry are replaced by the specified value. |
| [`RT_CubeBurn`](docs/functions.md#rt_cubeburn) | Returns a datacube where cells inside the given geometry are replaced by the specified value. |
| [`RT_CoordValue`](docs/functions.md#rt_coordvalue) | Returns the value in a datacube at the pixel coordinates corresponding to the given spatial coordinates. |
| [`RT_RasterValue`](docs/functions.md#rt_rastervalue) | Returns the value in a datacube at the specified pixel coordinates. |

**[Aggregate Functions](docs/functions.md#aggregate-functions)**

Aggregate functions operate on groups of rows (e.g. from a `GROUP BY` query) and return a single value per group.

| Function | Summary |
| --- | --- |
| [`RT_CubeStats_Agg`](docs/functions.md#rt_cubestats_agg) | Calculates statistics for a specific band (0-based index) in a set of datacubes. |
| [`RT_Envelope_Agg`](docs/functions.md#rt_envelope_agg) | Computes the bounding box of the valid (non-no-data) cells in a set of datacubes and returns it as a geometry. |
| [`RT_Polygon_Agg`](docs/functions.md#rt_polygon_agg) | Creates a polygon geometry for each contiguous region of non-no-data values in a set of datacubes. |
| [`RT_CoordValue_Agg`](docs/functions.md#rt_coordvalue_agg) | Returns the value in a set of datacubes at the pixel coordinates corresponding to the given spatial coordinates. |
| [`RT_RasterValue_Agg`](docs/functions.md#rt_rastervalue_agg) | Returns the value in a set of datacubes at the specified pixel coordinates. |

## Examples

More examples are available in the [examples guide](docs/examples.md) and in the [SQL tests](test/sql) used by the CI pipeline.

### Listing available drivers

```sql
SELECT short_name, long_name, help_url FROM RT_Drivers();
```
```sql
┌────────────────┬──────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┐
│   short_name   │                        long_name                         │                      help_url                       │
│    varchar     │                         varchar                          │                       varchar                       │
├────────────────┼──────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────┤
│ VRT            │ Virtual Raster                                           │ https://gdal.org/drivers/raster/vrt.html            │
│ GTiff          │ GeoTIFF                                                  │ https://gdal.org/drivers/raster/gtiff.html          │
│ COG            │ Cloud optimized GeoTIFF generator                        │ https://gdal.org/drivers/raster/cog.html            │
│  ·             │          ·                                               │                    ·                                │
│  ·             │          ·                                               │                    ·                                │
│  ·             │          ·                                               │                    ·                                │
│ ENVI           │ ENVI .hdr Labelled                                       │ https://gdal.org/drivers/raster/envi.html           │
│ Zarr           │ Zarr                                                     │ NULL                                                │
│ HTTP           │ HTTP Fetching Wrapper                                    │ NULL                                                │
└────────────────┴──────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────┘
```

### Reading a raster file (or a mosaic of raster files)

```sql
SELECT * FROM RT_Read('path/to/raster/file.tif');
```
```sql
┌───────┬───────────┬────────────┬────────────────────────────────┬─────────────────────────┬───────┬────────┬────────┬───────┬───────┬────────────┬────────────┐
│  id   │     x     │     y      │              bbox              │        geometry         │ level │ tile_x │ tile_y │ cols  │ rows  │  metadata  │ databand_1 │
│ int64 │  double   │   double   │ struct(xmin, ymin, xmax, ymax) │ geometry('epsg:25830')  │ int32 │ int32  │ int32  │ int32 │ int32 │    JSON    │    BLOB    │
├───────┼───────────┼────────────┼────────────────────────────────┼─────────────────────────┼───────┼────────┼────────┼───────┼───────┼────────────┼────────────┤
│     0 │ 545619.75 │ 4724508.25 │ {xmin: 545539.75,              │ POLYGON ((...))         │     0 │      0 │      0 │   320 │     8 │ {...}      │ ...        │
│       │           │            │  ymin: 4724506.25,             │                         │       │        │        │       │       │            │            │
│       │           │            │  xmax: 545699.75,              │                         │       │        │        │       │       │            │            │
│       │           │            │  ymax: 4724510.25}             │                         │       │        │        │       │       │            │            │
│     1 │ 545619.75 │ 4724504.25 │ {xmin: 545539.75,              │ POLYGON ((...))         │     0 │      0 │      1 │   320 │     8 │ {...}      │ ...        │
│       │           │            │  ymin: 4724502.25,             │                         │       │        │        │       │       │            │            │
│       │           │            │  xmax: 545699.75,              │                         │       │        │        │       │       │            │            │
│       │           │            │  ymax: 4724506.25}             │                         │       │        │        │       │       │            │            │
└───────┴───────────┴────────────┴────────────────────────────────┴─────────────────────────┴───────┴────────┴────────┴───────┴───────┴────────────┴────────────┘
```

```sql
-- Read multiple raster files as a mosaic using a VRT dataset
SELECT
    geometry, databand_1
FROM
    RT_Read([
        'path/to/mosaic/raster-clip00.tif',
        'path/to/mosaic/raster-clip01.tif',
        'path/to/mosaic/raster-clip10.tif',
        'path/to/mosaic/raster-clip11.tif'
    ])
;
```

```sql
LOAD spatial;

-- Filter tiles by spatial location
SELECT
	x, y, bbox, geometry
FROM
	RT_Read('path/to/raster/file.tif')
WHERE
	ST_Intersects(geometry, ST_GeomFromText('POLYGON((...)))'))
;
```

```sql
LOAD spatial;
LOAD json;

-- Vectorize valid (non-nodata) pixel regions into polygon geometries
SELECT
    RT_Polygon(databand_1,
               tile_x,
               tile_y,
              (metadata->'blocksize_x')::INTEGER,
              (metadata->'blocksize_y')::INTEGER,
              (metadata->'transform')::DOUBLE[]) AS geometry
FROM
    RT_Read('path/to/raster/file.tif')
;
```

### Writing a raster file

```sql
COPY (
	SELECT
		geometry, databand_1, databand_2, databand_3
	FROM
		RT_Read('./input.tiff')
)
TO './output.tiff'
WITH (
	FORMAT RASTER,
	DRIVER 'COG',
	CREATION_OPTIONS ('COMPRESS=LZW'),
	RESAMPLING 'nearest',
	-- ENVELOPE [545539.750, 4724420.250, 545699.750, 4724510.250],  -- explicit extent (optional)
	COMPUTE_VALID_ENVELOPE true,  -- derive extent from valid pixels
	SRS 'EPSG:25830',
	GEOMETRY_COLUMN 'geometry',
	DATABAND_COLUMNS ['databand_3', 'databand_2', 'databand_1']
);
```

### Band algebra

```sql
WITH __input AS (
	SELECT
		RT_Cube2ArrayFloat(databand_1, true) AS band
	FROM
		RT_Read('path/to/raster/file.tif', blocksize_x := 512, blocksize_y := 512)
)
SELECT
	list_min(band.values) AS band_min,
	list_stddev_pop(band.values) AS band_stddev,
	list_max(band.values) AS band_max
FROM
	__input
;
```

```sql
WITH __input AS (
	SELECT
		databand_1 AS red,
		databand_3 AS nir
	FROM
		RT_Read('path/to/raster/file.tif', blocksize_x := 512, blocksize_y := 512)
)
SELECT
	RT_Cube2TypeFloat((nir - red) / (nir + red)) AS ndvi
FROM
	__input
;
```

For the full function reference and all available options, see [docs/functions.md](docs/functions.md).

## How do I build it?

This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).

### Dependencies

You need CMake ≥ 3.5 and a C++14-compatible compiler. [Ninja](https://ninja-build.org) is recommended and can be selected by setting `GEN=ninja`.

```sh
git clone --recurse-submodules https://github.com/ahuarte47/duckdb-raster
cd duckdb-raster
make release
```

Invoke the built DuckDB (with the extension statically linked):

```sh
./build/release/duckdb
```

See the Makefile or the [extension template documentation](https://github.com/duckdb/extension-template) for additional options.

### Running the tests

SQL tests live in `./test/sql` and are the primary test suite for the extension:

```sh
make test
```

### Installing a locally built binary

To load an unsigned local build, launch DuckDB with `allow_unsigned_extensions` enabled:

CLI:
```sh
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions': 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', { allow_unsigned_extensions: 'true' });
```

Then load the extension from its local path:
```sql
LOAD 'build/release/extension/raster/raster.duckdb_extension';
```
