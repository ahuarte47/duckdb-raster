#pragma once

// DuckDB
#include "duckdb/common/types.hpp"
#include "duckdb/common/limits.hpp"

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

//! Column indices for the implicit columns of the cells table function.
#define CELL_ROWID_COLUMN_INDEX      0
#define CELL_PIXEL_X_COLUMN_INDEX    1
#define CELL_PIXEL_Y_COLUMN_INDEX    2
#define CELL_COORD_X_COLUMN_INDEX    3
#define CELL_COORD_Y_COLUMN_INDEX    4
#define CELL_GEOMETRY_COLUMN_INDEX   5
#define CELL_FIRST_BAND_COLUMN_INDEX 6

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

	bool operator!=(const RasterCoord &other) const {
		return col != other.col || row != other.row;
	}
	bool operator==(const RasterCoord &other) const {
		return col == other.col && row == other.row;
	}
};

//! Transformation matrix for mapping between Raster coordinates and World coordinates.
struct RasterTransformMatrix {
	double affine[6] = {0, 1, 0, 0, 0, -1}; // Default to identity transform with y flipped (i.e. pixel coordinates)
	int32_t blocksize_x = 128;
	int32_t blocksize_y = 128;

	bool operator!=(const RasterTransformMatrix &other) const {
		for (int i = 0; i < 6; i++) {
			if (affine[i] != other.affine[i]) {
				return true;
			}
		}
		return blocksize_x != other.blocksize_x || blocksize_y != other.blocksize_y;
	}
	bool operator==(const RasterTransformMatrix &other) const {
		return !(*this != other);
	}
};

//! Bounding box of a raster in pixel coordinates.
struct RasterBounds {
	int32_t min_col = NumericLimits<int32_t>::Maximum();
	int32_t max_col = NumericLimits<int32_t>::Minimum();
	int32_t min_row = NumericLimits<int32_t>::Maximum();
	int32_t max_row = NumericLimits<int32_t>::Minimum();

	RasterBounds()
	    : min_col(NumericLimits<int32_t>::Maximum()), max_col(NumericLimits<int32_t>::Minimum()),
	      min_row(NumericLimits<int32_t>::Maximum()), max_row(NumericLimits<int32_t>::Minimum()) {
	}

	//! Expand the bounding box to include the given column and row.
	void Grow(int32_t col, int32_t row) {
		if (col < min_col) {
			min_col = col;
		}
		if (col > max_col) {
			max_col = col;
		}
		if (row < min_row) {
			min_row = row;
		}
		if (row > max_row) {
			max_row = row;
		}
	}
	//! Expand the bounding box to include the given coordinate.
	void Grow(const RasterCoord &coord) {
		Grow(coord.col, coord.row);
	}

	//! Returns true if the bounding box is empty (i.e. has no valid cells).
	bool IsEmpty() const {
		return min_col > max_col || min_row > max_row;
	}
};

class ExtensionLoader;

//! Define new types to register into DuckDB.
struct RasterTypes {
	static LogicalType BBOX();
	static LogicalType DATACUBE();
	static LogicalType ARRAY(const LogicalType &element_type);
	static LogicalType STATS();

	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
