# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(raster
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
    EXTENSION_VERSION 1.7.1
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 516ad39e8d147f4e452bb659b129535d575b1b54
)

# Since DuckDB's v2.0-cyanoptera duckdb_extension_load() only decides what is built;
# the shell and the test runner link an extension only when a config asks for it.
# Linked as before the change.
duckdb_extension_statically_link(raster)
