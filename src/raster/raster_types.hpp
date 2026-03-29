#pragma once

#include <cstdint>
#include <string>
#include <stdexcept>
#include "duckdb/common/limits.hpp"

namespace duckdb {

//! Magic code to identify a BLOB as a raster block (ASCII "RS" = 0x5253).
#define RASTER_BLOCK_HEADER_MAGIC ((uint16_t)0x5253)

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

//! Compression algorithm for the tile data.
struct CompressionAlg {
	enum Value : uint8_t {
		NONE = 0,
	};

	//! Convert a string to a CompressionAlg::Value.
	static CompressionAlg::Value FromString(const std::string &compression_alg) {
		if (compression_alg == "none") {
			return NONE;
		} else {
			throw std::runtime_error("Unknown CompressionAlgorithm: " + compression_alg);
		}
	}
	//! Convert a CompressionAlg::Value to a string.
	static std::string ToString(Value compression_alg) {
		switch (compression_alg) {
		case NONE:
			return "none";
		default:
			throw std::runtime_error("Unknown CompressionAlgorithm: " + std::to_string(compression_alg));
		}
	}
};

//! Header for the raster tile data stored in the BLOB column.
struct TileHeader {
	uint16_t magic;                    // Magic code to identify a BLOB as a raster block (RASTER_BLOCK_HEADER_MAGIC)
	CompressionAlg::Value compression; // Compression algorithm used for the tile data
	uint8_t data_type;                 // GDALDataType of the tile data (Byte, UInt16, Float32, etc)
	int32_t bands;                     // Number of bands or layers in the data buffer
	int32_t cols;                      // Number of columns in the data buffer
	int32_t rows;                      // Number of rows in the data buffer
	double no_data;                    // NoData value for the tile (To consider when applying algebra operations)

	TileHeader()
	    : magic(RASTER_BLOCK_HEADER_MAGIC), compression(CompressionAlg::NONE), data_type(0), bands(0), cols(0), rows(0),
	      no_data(NumericLimits<double>::Minimum()) {
	}
	TileHeader(CompressionAlg::Value compression, uint8_t data_type, int32_t bands, int32_t cols, int32_t rows,
	           double no_data)
	    : magic(RASTER_BLOCK_HEADER_MAGIC), compression(compression), data_type(data_type), bands(bands), cols(cols),
	      rows(rows), no_data(no_data) {
	}
};

class ExtensionLoader;
struct LogicalType;

//! Define new types to register into DuckDB.
struct RasterTypes {
	static LogicalType BBOX();

	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
