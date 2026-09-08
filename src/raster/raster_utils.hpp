#pragma once

#include <cinttypes>

#include "raster_types.hpp"
#include "data_types.hpp"

// DuckDB
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/geometry.hpp"
// GDAL
#include "gdal_priv.h"

// Debug logging controlled by RASTER_DEBUG environment variable
#if defined(__has_cpp_attribute) && __has_cpp_attribute(maybe_unused)
[[maybe_unused]]
#endif
static int
GetDebugLevel() {
	static int level = -1;
	if (level == -1) {
		const char *env = std::getenv("RASTER_DEBUG");
		level = env ? std::atoi(env) : 0;
	}
	return level;
}

#define RASTER_SCAN_DEBUG_LOG(level, fmt, ...)                                                                         \
	do {                                                                                                               \
		if (GetDebugLevel() >= level) {                                                                                \
			fprintf(stderr, "RASTER: " fmt "\n", ##__VA_ARGS__);                                                       \
		}                                                                                                              \
	} while (0)

namespace duckdb {

//! Utility class for raster data handling.
class RasterUtils {
public:
	//! Get the last error message from GDAL as a string.
	static std::string GetLastGdalErrorMsg();

	//! Convert raster raster coordinate to geographic world coordinate.
	static Point2D RasterCoordToWorldCoord(const double (&matrix)[6], const int32_t &col, const int32_t &row);
	//! Convert raster raster coordinate to geographic world coordinate.
	static Point2D RasterCoordToWorldCoord(const double (&matrix)[6], const RasterCoord &coord);

	//! Convert geographic world coordinate to raster raster coordinate.
	static RasterCoord WorldCoordToRasterCoord(const double (&matrix)[6], const double &x, const double &y);
	//! Convert geographic world coordinate to raster raster coordinate.
	static RasterCoord WorldCoordToRasterCoord(const double (&matrix)[6], const Point2D &coord);

	//! Convert a raster coordinate window to a geographic world coordinate extent.
	static GeometryExtent RasterRectToWorldRect(const double (&matrix)[6], int32_t min_col, int32_t min_row,
	                                            int32_t max_col, int32_t max_row);
	//! Convert a raster coordinate window to a geographic world coordinate extent.
	static GeometryExtent RasterRectToWorldRect(const double (&matrix)[6], const RasterBounds &bounds);

	//! Convert a geographic world coordinate extent to a raster coordinate window.
	static RasterBounds WorldRectToRasterRect(const double (&matrix)[6], int raster_size_x, int raster_size_y,
	                                          double x_min, double y_min, double x_max, double y_max);
	//! Convert a geographic world coordinate extent to a raster coordinate window.
	static RasterBounds WorldRectToRasterRect(const double (&matrix)[6], int raster_size_x, int raster_size_y,
	                                          const GeometryExtent &bounds);

	//! Get the metadata of a GDAL dataset as a JSON string.
	static std::string GetMetadataOfDataset(GDALDataset *dataset);

	//! Build a RasterTransformMatrix from tile metadata.
	static RasterTransformMatrix GetTransformMatrix(const string &metadata);

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

	//! Convert a GDALDataType to the corresponding LogicalType.
	static LogicalType GdalTypeToLogicalType(const GDALDataType &data_type);
};

} // namespace duckdb
