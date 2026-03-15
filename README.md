# DuckDB Raster Extension

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

### Supported Functions and Documentation

The full list of functions and their documentation is available in the [function reference](docs/functions.md)

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
