#pragma once

// DuckDB
#include "duckdb/common/types.hpp"

namespace duckdb {

//! Column indices for the implicit columns of the raster table function.
#define RASTER_ROWID_COLUMN_INDEX      0
#define RASTER_X_COLUMN_INDEX          1
#define RASTER_Y_COLUMN_INDEX          2
#define RASTER_BBOX_COLUMN_INDEX       3
#define RASTER_GEOMETRY_COLUMN_INDEX   4
#define RASTER_LEVEL_COLUMN_INDEX      5
#define RASTER_COL_COLUMN_INDEX        6
#define RASTER_ROW_COLUMN_INDEX        7
#define RASTER_WIDTH_COLUMN_INDEX      8
#define RASTER_HEIGHT_COLUMN_INDEX     9
#define RASTER_METADATA_COLUMN_INDEX   10
#define RASTER_FIRST_BAND_COLUMN_INDEX 11

//! Geographic 2D coordinate.
struct Point2D {
	double x;
	double y;
	explicit Point2D(double x = 0.0, double y = 0.0) : x(x), y(y) {
	}
};

//! Position of a cell in a Raster (upper left corner as column and row).
struct RasterCoord {
	int32_t col;
	int32_t row;
	explicit RasterCoord(int32_t col = 0, int32_t row = 0) : col(col), row(row) {
	}
};

class ExtensionLoader;

//! Define new types to register into DuckDB.
struct RasterTypes {
	static LogicalType BBOX();
	static LogicalType DATACUBE();
	static LogicalType ARRAY(const LogicalType &element_type);

	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
