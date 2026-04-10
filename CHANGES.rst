Release history
---------------

0.4.0 (WIP)
++++++++++++++++++

- Update to GDAL 3.12.3 & support for more raster formats.
- Fix `RT_Read` function to handle nodata values NaN correctly.

0.3.1
++++++++++++++++++

- Fixed function `RT_Read` when reading big raster files.
- Added `skip_empty_tiles` parameter to `RT_Read` to skip empty tiles (It checks `GDAL_DATA_COVERAGE_STATUS_DATA` flag if supported).

0.3.0
++++++++++++++++++

- The function `RT_Read` supports Filter pushdown.

0.2.0
++++++++++++++++++

- New `RT_Blob2Array<TYPE>` functions to transform the data band BLOBs into ARRAYS of the corresponding type.

0.1.0
++++++++++++++++++

- First release as DuckDB Community Extension.
