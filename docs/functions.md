# DuckDB Raster Extension Function Reference

## Function Index

**[Table Functions](#table-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Drivers`](#rt_drivers) | Returns the list of supported GDAL RASTER drivers and file formats. |
| [`RT_Read`](#rt_read) | Reads a raster file and returns a table with the raster data. |
| [`COPY TO`](#rt_write) | Exports the data table to a new raster file. |

**[Scalar Functions](#scalar-functions)**

| Function | Summary |
| --- | --- |
| [`RT_Cube2Array`](#rt_cube2array) | Transforms the data of the datacube columns into an array of a numeric data type. |
| [`RT_Cube2Type`](#rt_cube2type) | Transforms a datacube column to another data type. |
| [`RT_Array2Cube`](#rt_array2cube) | Transforms an array of numeric values into a datacube column. |
| [`RT_CubeUnaryOp`](#rt_cubeunaryop) | Applies an unary operation to the values in the datacube element-wise. |
| [`RT_CubeBinaryOp`](#rt_cubebinaryop) | Applies a binary operation to the values in the datacube element-wise. |

----

## Table Functions

### RT_Drivers

Returns the list of supported GDAL RASTER drivers and file formats.

Note that not all of these drivers have been thoroughly tested.
Some may require additional options to be passed to work as expected.
If you run into any issues, please consult the [GDAL docs](https://gdal.org/drivers/raster/index.html).

#### Signature

```sql
RT_Drivers ()
```

#### Examples

```sql
SELECT * FROM RT_Drivers();
```

----

### RT_Read

The `RT_Read` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading raster data from a variety of geospatial raster file formats as if they were DuckDB tables.

> See [RT_Drivers](#rt_drivers) for a list of supported file formats and drivers.

The table returned by `RT_Read` is a tiled representation of the raster file, where each row corresponds to a tile of the raster. The tile size is determined by the original block size of the raster file, but it can be overridden by the user using the `blocksize_x` and `blocksize_y` parameters. The `geometry` column is a `GEOMETRY` of type `POLYGON` that represents the footprint of each tile, and you can use it to create a new geoparquet file by adding the option `GEOPARQUET_VERSION`.

Within this extension, the `databand` and `datacube` terms refer to the same underlying structure: an N-dimensional BLOB array holding the pixel values of one or more raster bands. A `databand` column refers to data for a single band, while a `datacube` column interleaves all bands into one. By default, `RT_Read` returns one `databand` column per band (`databand_1`, `databand_2`, etc.), but when the `datacube` option is set to `true`, it returns a single `datacube` column with all bands interleaved.

The `RT_Read` function accepts parameters, most of them optional:

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `path` | VARCHAR | The path to the file to read. The only mandatory parameter. |
| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. Refer to the GDAL documentation for available options. |
| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. |
| `data_format` | VARCHAR | The data format to use when packing the databand column. `RAW` is the only option currently supported. |
| `blocksize_x` | INTEGER | The block size of the tile in the x direction. You can use this parameter to override the original block size of the raster. |
| `blocksize_y` | INTEGER | The block size of the tile in the y direction. You can use this parameter to override the original block size of the raster. |
| `skip_empty_tiles` | BOOLEAN | `true` means that empty tiles (tiles with no data) will be skipped (It checks `GDAL_DATA_COVERAGE_STATUS_DATA` flag if supported). `true` is the default. |
| `datacube` | BOOLEAN | `true` means that the extension returns a single N-dimensional datacube column with all bands interleaved; otherwise each band is returned as a separate column. `false` is the default. |

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
	+ `data_format` (uint8_t): Data format code used for packing tile data:

		| Code | Key | Description |
		|------|-----------|-------------|
		| 0    | RAW | Uncompressed raw data, interleaved by pixel and band |

	+ `data_type` (uint8_t): Data type of the values of the tile data:

		| Code | Key | Description |
		|------|-----------|-------------|
		| 0    | UINT8 | Eight bit unsigned integer |
		| 1    | INT8 | 8-bit signed integer |
		| 2    | UINT16 | Sixteen bit unsigned integer |
		| 3    | INT16 | Sixteen bit signed integer |
		| 4    | UINT32 | Thirty two bit unsigned integer |
		| 5    | INT32 | Thirty two bit signed integer |
		| 6    | UINT64 | 64 bit unsigned integer |
		| 7    | INT64 | 64 bit signed integer |
		| 8    | FLOAT | Thirty two bit floating point |
		| 9    | DOUBLE | Sixty four bit floating point |

	+ `bands` (int32_t): Number of bands or layers in the data buffer.
	+ `cols` (int32_t): Number of columns in the data buffer.
	+ `rows` (int32_t): Number of rows in the data buffer.
	+ `no_data` (double): NoData value for the tile (to be considered when applying algebraic operations). `-infinity` if not defined.

+ `data`[] (uint8_t): Interleaved pixel data for all bands, stored in row-major order. The size of this array depends on the data type, number of bands, and tile dimensions.

By using `RT_Read`, the extension also provides “replacement scans” for common raster file formats, allowing you to query files of these formats as if they were tables directly.

`RT_Read` supports filter pushdown on the non-BLOB columns, which allows you to prefilter the tiles that are loaded based on their metadata or spatial location.
Note that the `bbox` and `geometry` columns are available for spatial filtering; for example, you can filter tiles that intersect a given geometry.

#### Signature

```sql
RT_Read (file_path VARCHAR,
         open_options VARCHAR[] DEFAULT NULL,
         allowed_drivers VARCHAR[] DEFAULT NULL,
         sibling_files VARCHAR[] DEFAULT NULL,
         data_format VARCHAR DEFAULT 'RAW',
         blocksize_x INTEGER DEFAULT NULL,
         blocksize_y INTEGER DEFAULT NULL,
         datacube BOOLEAN DEFAULT false
         )
```

#### Examples

```sql
SELECT * FROM RT_Read('path/to/raster/file.tif');
```

----

### RT_Write

Aka `COPY TO` with `FORMAT RASTER`

You can write raster files using the `COPY` command in DuckDB.

The extension fetches the geometry and data band columns of the input table and creates a raster file with the desired properties. The `geometry` column is used to calculate the spatial extent and the resolution of the output raster, and the databand columns are used to populate the pixel values of the output raster.

The extension provides the format `RASTER` and a set of options to control the writing process:

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `FORMAT` | VARCHAR | Must be set to 'RASTER' to use the raster writing functionality. |
| `DRIVER` | VARCHAR | The GDAL driver to use to write the raster file. You can check available drivers using `RT_Drivers` function. |
| `CREATION_OPTIONS` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the creation of the file. Read GDAL documentation for available options. |
| `RESAMPLING` | VARCHAR | The resampling method to use when the tile size of the input data does not match the block size of the output raster. Available options are `nearest`, `bilinear`, `cubic`, `cubicspline`, `lanczos`, `average`, `mode`.... `nearest` is the default. |
| `ENVELOPE` | DOUBLE[] | The spatial extent of the output raster in the format [xmin, ymin, xmax, ymax]. If not provided, the extent will be calculated from the input tiles. |
| `SRS` | VARCHAR | The spatial reference system of the output raster in WKT or EPSG code format. |
| `GEOMETRY_COLUMN` | VARCHAR | The name of the column that contains the geometry of the tiles. This column will be used to calculate the spatial extent and resolution of the output raster. It must be a column of type `GEOMETRY`. `geometry` is the default name. |
| `DATABAND_COLUMNS` | VARCHAR[] | A list of columns that contain the data bands of the raster. These columns must be of type BLOB and have the same internal structure as the data band columns returned by `RT_Read`. |

Raster rotation is not supported, so the input `geometry` column must contain axis-aligned polygons that represent the footprint of each tile.

#### Signature

```sql
COPY (
	SELECT geometry, databand_1, ...
)
TO 'path/to/output/file.tif'
WITH (
	FORMAT RASTER,
	...
);
```

#### Examples

You can write a new raster file from an existing one by running:

```sql
COPY (
   	SELECT
		geometry,
		databand_1, databand_2, databand_3
	FROM
		RT_Read('./test/data/overlay-sample.tiff')
)
TO './test/data/copytoraster.tiff'
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

Also, because the `geometry` column is available, you can create a new `geoparquet` file (or any other geospatial
format supported by the `spatial` extension) with the tile data and their geometries by just running:

```sql
COPY (
   	SELECT
		* EXCLUDE(databand_1,databand_2,databand_3)
   	FROM
		RT_Read('path/to/raster/file.tif')
)
TO 'path/to/output/file.parquet'
WITH (
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
TO 'path/to/output/file.gpkg'
WITH (
	FORMAT GDAL, DRIVER 'GPKG', SRS 'EPSG:4326'
);
```

----

## Scalar Functions

### RT_Cube2Array

Transforms the BLOB data of the data band columns into an array of a numeric data type.

Function accepts the following parameters:

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `blob` | BLOB | The column of the data band to transform. |
| `filter_nodata` | BOOLEAN | Whether to filter out NoData values from the array. If `true`, the function will exclude NoData values from the resulting array. |

Extension provides a different function for each numeric data type:

| Function | Description |
| -------- | ----------- |
| `RT_Cube2ArrayUInt8` | Transforms a BLOB data column into an array of UINT8 values |
| `RT_Cube2ArrayInt8` | Transforms a BLOB data column into an array of INT8 values |
| `RT_Cube2ArrayUInt16` | Transforms a BLOB data column into an array of UINT16 values |
| `RT_Cube2ArrayInt16` | Transforms a BLOB data column into an array of INT16 values |
| `RT_Cube2ArrayUInt32` | Transforms a BLOB data column into an array of UINT32 values |
| `RT_Cube2ArrayInt32` | Transforms a BLOB data column into an array of INT32 values |
| `RT_Cube2ArrayUInt64` | Transforms a BLOB data column into an array of UINT64 values |
| `RT_Cube2ArrayInt64` | Transforms a BLOB data column into an array of INT64 values |
| `RT_Cube2ArrayFloat` | Transforms a BLOB data column into an array of FLOAT values |
| `RT_Cube2ArrayDouble` | Transforms a BLOB data column into an array of DOUBLE values |

Functions return a struct with the following fields:

+ `data_type` (INT): DataType code of the data in the BLOB.
+ `bands` (INT): Number of bands or layers in the data buffer.
+ `cols` (INT): Number of columns in the tile.
+ `rows` (INT): Number of rows in the tile.
+ `no_data` (DOUBLE): NoData value for the tile (to be considered when applying algebraic operations). `-infinity` if not defined.
+ `values` (ARRAY): An array with the pixel values of the tile for the corresponding band and data type.

Casting from the BLOB format to an array format is implemented as well, so you can also use a simple cast to transform a
data band column into an array of values.

#### Signature

```sql
RT_Cube2Array<data_type> (databand DATACUBE, filter_nodata BOOLEAN)
```

#### Examples

```sql
SELECT
	RT_Cube2ArrayInt32(databand_1, true) AS r,
	RT_Cube2ArrayInt32(databand_2, true) AS g,
	RT_Cube2ArrayInt32(databand_3, true) AS b
FROM
	RT_Read('path/to/raster/file.tif')
;
```

This function set allows you to perform operations on the tile data directly in SQL:

```sql
WITH __input AS (
	SELECT
		RT_Cube2ArrayInt32(databand_1, false) AS r
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

Choose carefully which `RT_Cube2Array<data_type>` function to invoke; if the array element type in the output
does not match the data type in the data band column, the function needs to adjust values accordingly,
and performance may be affected. You can check the data type of the bands in the `metadata` column
returned by `RT_Read`.

You can use a simple cast to transform a data band column into an array of values, but note that in this
case the function does not filter out NoData values from the resulting array:

```sql
SELECT
	databand_1::DOUBLE[] AS r_array,
FROM
	RT_Read('path/to/raster/file.tif')
;
```

----

### RT_Cube2Type

Transforms a datacube column to another data type.

This function allows you to transform the data type of a datacube column to another data type, for example,
to convert a datacube with `INT16` values into a datacube with `FLOAT` values.

All algebraic operations on the datacube columns return a datacube with `DOUBLE` data type, so you can use
this function to cast the results of these operations back to the original data type of the raster file,
or for example, to write the results into a new raster file with the desired data type.

Function accepts the following parameters:

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `blob` | BLOB | The column of the data band to transform. |

Extension provides a different function for each numeric data type:

| Function | Description |
| -------- | ----------- |
| `RT_Cube2TypeUInt8` | Transforms a datacube column into a datacube with UINT8 values |
| `RT_Cube2TypeInt8` | Transforms a datacube column into a datacube with INT8 values |
| `RT_Cube2TypeUInt16` | Transforms a datacube column into a datacube with UINT16 values |
| `RT_Cube2TypeInt16` | Transforms a datacube column into a datacube with INT16 values |
| `RT_Cube2TypeUInt32` | Transforms a datacube column into a datacube with UINT32 values |
| `RT_Cube2TypeInt32` | Transforms a datacube column into a datacube with INT32 values |
| `RT_Cube2TypeUInt64` | Transforms a datacube column into a datacube with UINT64 values |
| `RT_Cube2TypeInt64` | Transforms a datacube column into a datacube with INT64 values |
| `RT_Cube2TypeFloat` | Transforms a datacube column into a datacube with FLOAT values |
| `RT_Cube2TypeDouble` | Transforms a datacube column into a datacube with DOUBLE values |

#### Signature

```sql
RT_Cube2Type<data_type> (databand DATACUBE)
```

#### Examples

```sql
SELECT
	RT_Cube2TypeFloat(databand_1 / 1000) AS r_float,
	RT_Cube2TypeFloat(databand_2 / 1000) AS g_float,
	RT_Cube2TypeFloat(databand_3 / 1000) AS b_float
FROM
	RT_Read('path/to/raster/file.tif')
;
```

----

### RT_Array2Cube

Transforms an array of numeric values into a databand column (BLOB).

This function is the inverse of `RT_Cube2Array` and allows you to transform the results of algebraic operations
on the tile data back into a BLOB column with the internal structure required by `COPY` with `FORMAT RASTER`,
so you can write the results of your operations into a new raster file.

Function accepts the following parameters:

| Parameter | Type | Description |
| --------- | -----| ----------- |
| `array` | ARRAY | The array of numeric values to transform into a BLOB column. |
| `data_format` | VARCHAR | The data format to use for packing the data into the BLOB. |
| `bands` | INT | Number of bands or layers in the data buffer. |
| `cols` | INT | Number of columns in the tile. |
| `rows` | INT | Number of rows in the tile. |
| `no_data` | DOUBLE | NoData value for the tile (to be considered when applying algebraic operations). |

#### Signature

```sql
RT_Array2Cube (array ARRAY, data_format VARCHAR, bands INT, cols INT, rows INT, no_data DOUBLE)
```

#### Examples

```sql
WITH __input AS (
	SELECT
		RT_Cube2ArrayInt32(databand_1, false) AS r
	FROM
		RT_Read('path/to/raster/file.tif', blocksize_x := 512, blocksize_y := 512)
)
SELECT
	RT_Array2Cube(r.values, 'RAW', r.bands, r.cols, r.rows, r.no_data) AS r_array
FROM
	__input
;
```

----

### RT_CubeUnaryOp

Applies an unary operation to the values in the datacube element-wise.

| Function | Description |
| -------- | ----------- |
| `RT_CubeNeg` | Negate the values in the datacube element-wise. |
| `RT_CubeAbs` | Takes the absolute value of the values in the datacube element-wise. |
| `RT_CubeSqrt` | Takes the square root of the values in the datacube element-wise. |
| `RT_CubeLog` | Takes the logarithm of the values in the datacube element-wise. |
| `RT_CubeExp` | Takes the exponential of the values in the datacube element-wise. |

#### Signature

```sql
RT_Cube<funcname> (databand DATACUBE)
```

#### Examples

```sql
SELECT
	RT_CubeNeg(databand_1) AS neg
FROM
	RT_Read('path/to/raster/file.tif')
;
```

----

### RT_CubeBinaryOp

Applies a binary operation to the values in the datacube element-wise.

| Function | Description |
| -------- | ----------- |
| `RT_CubeEqual` | Return 1 where values in `datacube_a` are equal to `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeNotEqual` | Return 1 where values in `datacube_a` are not equal to `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeGreater` | Return 1 where values in `datacube_a` are greater than `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeGreaterEqual` | Return 1 where values in `datacube_a` are greater than or equal to `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeLess` | Return 1 where values in `datacube_a` are less than `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeLessEqual` | Return 1 where values in `datacube_a` are less than or equal to `datacube_b` or a scalar value, 0 otherwise. |
| `RT_CubeAdd` | Add the values in `datacube_a` to `datacube_b` or a scalar value element-wise. |
| `RT_CubeSubtract` | Subtract `datacube_b` or a scalar value from the values in `datacube_a` element-wise. |
| `RT_CubeMultiply` | Multiply the values in `datacube_a` by `datacube_b` or a scalar value element-wise. |
| `RT_CubeDivide` | Divide the values in `datacube_a` by `datacube_b` or a scalar value element-wise. |
| `RT_CubePow` | Take the power of the values in `datacube_a` to `datacube_b` or a scalar value element-wise. |

The math operators (`+`, `-`, `*`, `/`, `^`, `%`) are also supported as aliases of the corresponding functions.

#### Signature

```sql
RT_Cube<funcname> (databand_a DATACUBE, value_b [DATACUBE, double])
```

#### Examples

```sql
SELECT
	RT_CubeAdd(databand_1, 10) AS v1,
	RT_CubeAdd(databand_1, databand_2) AS v2,
	(v1 + v2) AS v3
FROM
	RT_Read('path/to/raster/file.tif')
;
```

----
