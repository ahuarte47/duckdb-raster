#pragma once

#include "raster_types.hpp"
#include "data_types.hpp"

// DuckDB
#include "duckdb/common/types.hpp"
// GDAL
#include "gdal_priv.h"

namespace duckdb {

//! Utility class for raster data handling.
class RasterUtils {
public:
	//! Get the last error message from GDAL as a string.
	static std::string GetLastGdalErrorMsg();

	//! Convert raster pixel coordinate to geographic 2D coordinate.
	static Point2D RasterCoordToWorldCoord(const double matrix[6], const int32_t &col, const int32_t &row);

	//! Get the SRID from a WKT projection definition, returns 0 if it cannot be determined.
	static int GetSrid(const char *proj_def);

	//! Convert a DuckDB LogicalType to the corresponding DataType.
	static DataType::Value LogicalTypeToDataType(const LogicalType &data_type);
	//! Convert a DataType to the corresponding DuckDB LogicalType.
	static LogicalType DataTypeToLogicalType(const DataType::Value &data_type);

	//! Convert a GDALDataType to the corresponding DataType.
	static DataType::Value GdalTypeToDataType(const GDALDataType &data_type);
	//! Convert a DataType to the corresponding GDALDataType.
	static GDALDataType DataTypeToGdalType(const DataType::Value &data_type);
};

} // namespace duckdb
