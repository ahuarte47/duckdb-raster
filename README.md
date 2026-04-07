# DuckDB Raster Extension

🚧 WORK IN PROGRESS 🚧

## What is this?

This is an extension for DuckDB for reading and writing raster files data using SQL.

## How do I get it?

### Loading from community (TODO)

The DuckDB **Raster Extension** is available as a signed [community extension](https://duckdb.org/community_extensions/list_of_extensions).
See more details on its [DuckDB CE web page](https://duckdb.org/community_extensions/extensions/raster.html).

To install and load it, you can run the following SQL commands in DuckDB:

```sql
INSTALL raster FROM community;
LOAD raster;
```

### Building from source

This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).

## Example Usage

First, make sure to load the extension in your DuckDB session.

Then you can use the extension to read and write raster files data using SQL.

This is the list of available functions:

+ ### RT_Drivers

    Returns the list of supported GDAL raster drivers.

    ```sql
	SELECT short_name, long_name, help_url FROM RT_Drivers();

	┌────────────────┬──────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────┐
	│   short_name   │                        long_name                         │                      help_url                       │
	│    varchar     │                         varchar                          │                       varchar                       │
	├────────────────┼──────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────┤
	│ VRT            │ Virtual Raster                                           │ https://gdal.org/drivers/raster/vrt.html            │
	│ DERIVED        │ Derived datasets using VRT pixel functions               │ https://gdal.org/drivers/raster/derived.html        │
	│ GTI            │ GDAL Raster Tile Index                                   │ https://gdal.org/drivers/raster/gti.html            │
	│ SNAP_TIFF      │ Sentinel Application Processing GeoTIFF                  │ https://gdal.org/drivers/raster/snap_tiff.html      │
	│ GTiff          │ GeoTIFF                                                  │ https://gdal.org/drivers/raster/gtiff.html          │
	│ COG            │ Cloud optimized GeoTIFF generator                        │ https://gdal.org/drivers/raster/cog.html            │
	│  ·             │          ·                                               │                    ·                                │
	│  ·             │          ·                                               │                    ·                                │
	│  ·             │          ·                                               │                    ·                                │
	│ ENVI           │ ENVI .hdr Labelled                                       │ https://gdal.org/drivers/raster/envi.html           │
	│ EHdr           │ ESRI .hdr Labelled                                       │ https://gdal.org/drivers/raster/ehdr.html           │
	│ ISCE           │ ISCE raster                                              │ https://gdal.org/drivers/raster/isce.html           │
	│ Zarr           │ Zarr                                                     │ NULL                                                │
	│ HTTP           │ HTTP Fetching Wrapper                                    │ NULL                                                │
	└────────────────┴──────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────┘
    ```

+ ### RT_Read

	Reads a raster file and returns a table with the raster data.

	```sql
	SELECT * FROM RT_Read('path/to/raster/file.tif');

	┌───────┬───────────┬────────────┬────────────────────────────────┬─────────────────────────┬───────┬────────┬────────┬───────┬───────┬────────────┬────────────┐
	│  id   │     x     │     y      │            bbox                │       geometry          │ level │ tile_x │ tile_y │ cols  │ rows  │  metadata  │ databand_1 │
	│ int64 │  double   │   double   │ struct(xmin, ymin, xmax, ymax) │  geometry('epsg:25830') │ int32 │ int32  │ int32  │ int32 │ int32 │    JSON    │    BLOB    │
	├───────┼───────────┼────────────┼────────────────────────────────┼─────────────────────────┼───────┼────────┼────────┼───────┼───────┤────────────┤────────────┤
	│     0 │ 545619.75 │ 4724508.25 │ {                              │      POLYGON ((...))    │     0 │      0 │      0 │   320 │     8 │ {...}      │ ...        │
	│       │           │            │   'xmin': 545539.75,           │                         │       │        │        │       │       │            │            │
	│       │           │            │   'ymin': 4724506.25,          │                         │       │        │        │       │       │            │            │
	│       │           │            │   'xmax': 545699.75,           │                         │       │        │        │       │       │            │            │
	│       │           │            │   'ymax': 4724510.25           │                         │       │        │        │       │       │            │            │
	│       │           │            │ }                              │                         │       │        │        │       │       │            │            │
	├───────┼───────────┼────────────┼────────────────────────────────┼─────────────────────────┼───────┼────────┼────────┼───────┼───────┤────────────┤────────────┤
	│     1 │ 545619.75 │ 4724504.25 │ {                              │      POLYGON ((...))    │     0 │      0 │      1 │   320 │     8 │ {...}      │ ...        │
	│       │           │            │   'xmin': 545539.75,           │                         │       │        │        │       │       │            │            │
	│       │           │            │   'ymin': 4724502.25,          │                         │       │        │        │       │       │            │            │
	│       │           │            │   'xmax': 545699.75,           │                         │       │        │        │       │       │            │            │
	│       │           │            │   'ymax': 4724506.25           │                         │       │        │        │       │       │            │            │
	│       │           │            │ }                              │                         │       │        │        │       │       │            │            │
	└───────┴───────────┴────────────┴────────────────────────────────┴─────────────────────────┴───────┴────────┴────────┴───────┴───────┴────────────┴────────────┘
	```

	The `RT_Read` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading raster data from a variety of geospatial raster file formats as if they were DuckDB tables.

	> See [RT_Drivers](#rt_drivers) for a list of supported file formats and drivers.

	The table returned by `RT_Read` is a tiled representation of the raster file, where each row corresponds to a tile of the raster. The tile size is determined by the original block size of the raster file, but it can be overridden by the user using the `blocksize_x` and `blocksize_y` parameters. `geometry` column is a `GEOMETRY` type of type `POLYGON` that represents the footprint of each tile and you can use it to create a new geoparquet file adding the option `GEOPARQUET_VERSION`.

	The `RT_Read` function accepts parameters, most of them optional:

	| Parameter | Type | Description |
	| --------- | -----| ----------- |
	| `path` | VARCHAR | The path to the file to read. The unique mandatory parameter. |
	| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. Read GDAL documentation for available options. |
	| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
	| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. |
	| `compression` | VARCHAR | The compression method to use when packing the databand column. `NONE` is the unique option now. |
	| `blocksize_x` | INTEGER | The block size of the tile in the x direction. You can use this parameter to override the original block size of the raster. |
	| `blocksize_y` | INTEGER | The block size of the tile in the y direction. You can use this parameter to override the original block size of the raster. |
	| `skip_empty_tiles` | BOOLEAN | `true` means that empty tiles (tiles with no data) will be skipped (It checks `GDAL_DATA_COVERAGE_STATUS_DATA` flag if supported). `true` is the default. |
	| `datacube` | BOOLEAN | `true` means that extension returns one unique N-dimensional databand column with all bands interleaved, otherwise each band is returned as a separate column. `false` is the default. |

	This is the list of columns returned by `RT_Read`:

	+ `id` is a unique identifier for each tile of the raster.
	+ `x` and `y` are the coordinates of the center of each tile. The coordinate reference system is the same as the one of the raster file.
	+ `bbox` is the bounding box of each tile, which is a struct with `xmin`, `ymin`, `xmax`, and `ymax` fields.
	+ `geometry` is the footprint of each tile as a polygon.
	+ `level`, `tile_x`, and `tile_y` are the tile coordinates of each tile. The raster is read in tiles of size `blocksize_x` x `blocksize_y` (or the original block size of the raster if not overridden by the parameters). Each row of the output table corresponds to a tile of the raster, and the `databand_x` columns contain the data of that tile for each band.
	+ `cols` and `rows` are the number of columns and rows of each tile, which can be different from the original raster if the `blocksize_x` and `blocksize_y` parameters are used to override the block size.
	+ `metadata` is a JSON column that contains the metadata of the raster file, including the list of bands and their properties (data type, no data value, etc), the spatial reference system, the geotransform, and any other metadata provided by the GDAL driver.
	+ `databand_x` are BLOB columns that contain the data of the raster bands and a header metadata describing the schema of the data. If the `datacube` option is set to `true`, only a single column called `datacube` will contain all bands interleaved in a single N-dimensional array.

	The data band columns are a BLOB with the following internal structure:

	+ A Header describes the raster tile data stored in the BLOB.
		+ `magic` (uint16_t): Magic code to identify a BLOB as a raster block (`0x5253`)
		+ `compression` (uint8_t): Compression algorithm code used for the tile data. `0=NONE` is the unique option now, but more can be added in the future.
		+ `data_type` (uint8_t): RasterDataType of the tile data:

			| Code | Data Type | Description |
			|------|-----------|-------------|
			| 0    | UNKNOWN | Unknown or unspecified type |
			| 1    | UINT8 | Eight bit unsigned integer |
			| 2    | INT8 | 8-bit signed integer |
			| 3    | UINT16 | Sixteen bit unsigned integer |
			| 4    | INT16 | Sixteen bit signed integer |
			| 5    | UINT32 | Thirty two bit unsigned integer |
			| 6    | INT32 | Thirty two bit signed integer |
			| 7    | UINT64 | 64 bit unsigned integer |
			| 8    | INT64 | 64 bit signed integer |
			| 9    | FLOAT | Thirty two bit floating point |
			| 10   | DOUBLE | Sixty four bit floating point |

		+ `bands` (int32_t): Number of bands or layers in the data buffer
		+ `cols` (int32_t): Number of columns in the data buffer
		+ `rows` (int32_t): Number of rows in the data buffer
		+ `no_data` (double): NoData value for the tile (To consider when applying algebra operations). `-infinity` if not defined.
	+ `data`[] (uint8_t): Interleaved pixel data for all bands, stored in row-major order. The size of this array depends on the data type, number of bands, and tile dimensions.


	By using `RT_Read`, the extension also provides “replacement scans” for common raster file formats, allowing you to query files of these formats as if they were tables directly.

	```sql
	SELECT * FROM './path/to/some/raster/dataset.tif';
	```

	In practice this is just syntax-sugar for calling `RT_Read`, so there is no difference in performance. If you want to pass additional options, you should use the `RT_Read` table function directly.

	The following formats are currently recognized by their file extension:

	| Format | Extension |
	| ------ | --------- |
	| GeoTiff COG | .tif, .tiff |
	| Erdas Imagine | .img |
	| GDAL Virtual | .vrt |

	`RT_Read` supports filter pushdown on the non-BLOB columns, which allows you to prefilter the tiles that are loaded based on their metadata or spatial location. As you have noticed, `bbox` and `geometry` columns are available for spatial filtering, so for example, you can filter the
	tiles that intersect with a certain geometry:

	```sql
	SELECT
		x, y, bbox, geometry
	FROM
		RT_Read('path/to/raster/file.tif')
	WHERE
		ST_Intersects(geometry, ST_GeomFromText('POLYGON((...))')::GEOMETRY('EPSG:4326'))
		AND
		ST_Area(geometry) > 1000
	;
	```

+ ### RT_Write (aka `COPY`)

	_TODO_: Add option to write raster files directly using `COPY`.

	Also, because the `geometry` column is available, you can create a new `geoparquet` file (or any other geospatial
	format supported by the `spatial` extension) with the tile data and their geometries by just running:

	```sql
	COPY (
    	SELECT
        	* EXCLUDE(databand_1,databand_2,databand_3)
    	FROM
        	RT_Read('path/to/raster/file.tif')
	)
	TO 'path/to/output/file.parquet' (
    	FORMAT PARQUET, GEOPARQUET_VERSION 'V1'
	);

	-- Or using the spatial extension, for example, writing a GeoPackage file:

	LOAD spatial;

	COPY (
		SELECT
			* EXCLUDE(databand_1,databand_2,databand_3)
		FROM
			RT_Read('path/to/raster/file.tif')
	)
	TO 'path/to/output/file.gpkg' (
		FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:4326'
	);
	```

+ ### RT_Blob2Array

	Transforms the BLOB data of the data band columns into an array of a numeric data type.

	```sql
	SELECT
		RT_Blob2ArrayInt32(databand_1, true) AS r,
		RT_Blob2ArrayInt32(databand_2, true) AS g,
		RT_Blob2ArrayInt32(databand_3, true) AS b
	FROM
		RT_Read('path/to/raster/file.tif')
	;
	```

	Function accepts the following parameters:

	| Parameter | Type | Description |
	| --------- | -----| ----------- |
	| `blob` | BLOB | The BLOB column of the data band to transform. |
	| `filter_nodata` | BOOLEAN | Whether to filter out NoData values from the array. If `true`, the function will exclude NoData values from the resulting array. |

	Extension provides a different function for each numeric data type:

	| Function | Description |
	| -------- | ----------- |
	| `RT_Blob2ArrayUInt8` | Transforms a BLOB data column into an array of UINT8 values |
	| `RT_Blob2ArrayInt8` | Transforms a BLOB data column into an array of INT8 values |
	| `RT_Blob2ArrayUInt16` | Transforms a BLOB data column into an array of UINT16 values |
	| `RT_Blob2ArrayInt16` | Transforms a BLOB data column into an array of INT16 values |
	| `RT_Blob2ArrayUInt32` | Transforms a BLOB data column into an array of UINT32 values |
	| `RT_Blob2ArrayInt32` | Transforms a BLOB data column into an array of INT32 values |
	| `RT_Blob2ArrayUInt64` | Transforms a BLOB data column into an array of UINT64 values |
	| `RT_Blob2ArrayInt64` | Transforms a BLOB data column into an array of INT64 values |
	| `RT_Blob2ArrayFloat` | Transforms a BLOB data column into an array of FLOAT values |
	| `RT_Blob2ArrayDouble` | Transforms a BLOB data column into an array of DOUBLE values |

	Functions return a struct with the following fields:

	+ `data_type` (INT): RasterDataType code of the data in the BLOB.
	+ `bands` (INT): Number of bands or layers in the data buffer.
	+ `cols` (INT): Number of columns in the tile.
	+ `rows` (INT): Number of rows in the tile.
	+ `no_data` (DOUBLE): NoData value for the tile (To consider when applying algebra operations). `-infinity` if not defined.
	+ `values` (ARRAY): An array with the pixel values of the tile for the corresponding band and data type.

	This allows you to do algebraic operations with the data of the tiles directly in SQL:

	```sql
	WITH __input AS (
		SELECT
			RT_Blob2ArrayInt32(databand_1, false) AS r
		FROM
			RT_Read('path/to/raster/file.tif', blocksize_x := 512, blocksize_y := 512)
	)
	SELECT
		list_min(r.values) AS r_min,
		list_stddev_pop(r.values) AS r_avg,
		list_max(r.values) AS r_max
	FROM
		__input
	;
	```

	Choose carefully which `RT_Blob2Array<type>` function you invoke, if the array element type in the output does
	not match the data type in the BLOB data column, the function need to adjust values accordingly, and the
	performance may	be affected. You can check the data type of the bands in the `metadata` column returned
	by `RT_Read`.

### Supported Functions and Documentation

The full list of functions and their documentation is available in the [function reference](docs/functions.md)

## TODO

This is the list of things I have in mind for the future, but if you want to contribute or have any suggestion please let me know!

+ `COPY` function to write raster files from the loaded tables.
+ Compression formats for the data band BLOBs (`GZip`, `ZSTD`?).
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
