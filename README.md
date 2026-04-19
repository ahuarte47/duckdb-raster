# DuckDB Raster Extension

🚧 WORK IN PROGRESS 🚧

## What is this?

This is an extension for DuckDB for reading and writing raster files data using SQL.

## How do I get it?

### Loading from community

The DuckDB **Raster Extension** is available as a signed [community extension](https://duckdb.org/community_extensions/list_of_extensions).
See more details on its [DuckDB CE web page](https://duckdb.org/community_extensions/extensions/raster.html).

To install and load it, you can run the following SQL commands in DuckDB:

```sql
INSTALL raster FROM community;
LOAD raster;
```

### Building from source

This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).

## Functions reference

After loading the extension, you can read and write raster files using SQL.

**[Table Functions](docs/functions.md#table-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Drivers`](docs/functions.md#rt_drivers) | Returns the list of supported GDAL raster drivers and file formats. |
| [`RT_Read`](docs/functions.md#rt_read) | Reads a raster file and returns a table with the raster data. |
| [`COPY TO`](docs/functions.md#rt_write) | Exports a data table to a new raster file. |

**[Scalar Functions](docs/functions.md#scalar-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Cube2Array`](docs/functions.md#rt_cube2array) | Transforms a databand BLOB column into an array of a numeric data type. |
| [`RT_Array2Cube`](docs/functions.md#rt_array2cube) | Transforms an array of numeric values into a databand BLOB column. |


#### Listing available drivers

```sql
SELECT short_name, long_name, help_url FROM RT_Drivers();

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

#### Reading a raster file

```sql
SELECT * FROM RT_Read('path/to/raster/file.tif');

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
-- Filter tiles by spatial location
SELECT
	x, y, bbox, geometry
FROM
	RT_Read('path/to/raster/file.tif')
WHERE
	ST_Intersects(geometry, ST_GeomFromText('POLYGON((...)))'))
;
```

#### Writing a raster file

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
	ENVELOPE [545539.750, 4724420.250, 545699.750, 4724510.250],
	SRS 'EPSG:25830',
	GEOMETRY_COLUMN 'geometry',
	DATABAND_COLUMNS ['databand_3', 'databand_2', 'databand_1']
);
```

#### Algebraic operations on band data

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

For the full function reference and all available options, see [docs/functions.md](docs/functions.md).

## TODO

This is the list of things I have in mind for the future, but if you want to contribute or have any suggestion please let me know!

+ Add basic math operations for the BLOB databand columns, like `+`, `-`, `*`, `/`.
+ Add compression formats for the BLOB databand columns (`GZip`, `ZSTD`?).
+ Integration with DuckDB File System.

## How do I build it?

### Dependencies

You need a recent version of CMake (3.5) and a C++14 compatible compiler.

We also highly recommend that you install [Ninja](https://ninja-build.org) which you can select when building by setting the `GEN=ninja` environment variable.
```
git clone --recurse-submodules https://github.com/ahuarte47/duckdb-raster
cd duckdb-raster
make release
```

You can then invoke the built DuckDB (with the extension statically linked)
```
./build/release/duckdb
```

Please see the Makefile for more options, or the extension template documentation for more details.

### Running the tests

Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL raster;
LOAD raster;
```

Enjoy!
