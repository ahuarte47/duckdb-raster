#pragma once

#include <cstdint>
#include <string>
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct RasterCoord;

//! Magic code to identify a BLOB as a raster block (ASCII "RS" = 0x5253).
#define DATA_BLOCK_HEADER_MAGIC ((uint16_t)0x5253)

//! Data type of a raster tile cell, equivalent to GDALDataType.
struct DataType {
	enum Value : uint8_t {
		UINT8 = 0,
		INT8 = 1,
		UINT16 = 2,
		INT16 = 3,
		UINT32 = 4,
		INT32 = 5,
		UINT64 = 6,
		INT64 = 7,
		FLOAT = 8,
		DOUBLE = 9
	};

	//! Get the size of a single value of the given data type in bytes.
	static int64_t GetSizeBytes(const DataType::Value &data_type);

	//! Convert a DataType::Value to a string.
	static std::string ToString(const DataType::Value &data_type);
};

//! Data format of a BLOB databand column, e.g. raw bytes, compressed, etc.
struct DataFormat {
	enum Value : uint8_t { RAW = 0 };

	//! Convert a string to a DataFormat.
	static DataFormat::Value FromString(const std::string &format_str);

	//! Convert a DataFormat to a string.
	static std::string ToString(const DataFormat::Value &data_format);
};

//! Header describing the tile data stored in the BLOB column.
class DataHeader {
public:
	uint16_t magic;                // Magic code identifying this BLOB as a raster block (DATA_BLOCK_HEADER_MAGIC)
	DataFormat::Value data_format; // Encoding format of the tile data (RAW, GZip, etc.)
	DataType::Value data_type;     // Pixel data type of the tile (UINT8, INT16, FLOAT, etc.)
	int32_t bands;                 // Number of bands (layers) in the data buffer
	int32_t cols;                  // Number of pixel columns in the tile
	int32_t rows;                  // Number of pixel rows in the tile
	double no_data;                // NoData sentinel value; cells equal to this are treated as missing

	DataHeader()
	    : magic(DATA_BLOCK_HEADER_MAGIC), data_format(DataFormat::Value::RAW), data_type(DataType::Value::UINT8),
	      bands(0), cols(0), rows(0), no_data(NumericLimits<double>::Minimum()) {
	}
	DataHeader(DataFormat::Value data_format, DataType::Value data_type, int32_t bands, int32_t cols, int32_t rows,
	           double no_data)
	    : magic(DATA_BLOCK_HEADER_MAGIC), data_format(data_format), data_type(data_type), bands(bands), cols(cols),
	      rows(rows), no_data(no_data) {
	}
};

//! A single cell in the data cube, carrying its linear position, its pixel value, and the nodata sentinel.
struct CubeCellValue {
	//! The position index of the cell in the cube.
	idx_t index;
	//! The value of the cell.
	double value;
	//! The no-data value of the cell, to consider when applying algebra operations.
	double no_data;

	//! Returns true if the given value equals the nodata sentinel (NaN-aware).
	static bool IsNoDataValue(double value, double no_data);
	//! Returns true if this cell's value equals its nodata sentinel (NaN-aware).
	bool IsNoDataValue() const;

	//! Check if a value is valid to be used in computations.
	static bool IsValidValue(double value, double no_data);
	//! Check if a value is valid to be used in computations.
	bool IsValidValue() const;

	//! Get the (col, row) coordinates of a cell in the tile.
	static RasterCoord GetCoord(idx_t index, idx_t bands, idx_t cols, idx_t rows);
	//! Get the (col, row) coordinates of this cell in the tile.
	RasterCoord GetCoord(const DataHeader &header) const;
};

//! Unary operations that can be applied to a data cube.
struct CubeUnaryOp {
	enum Value : uint8_t {
		//! Negate the value (multiply by -1).
		NEGATE = 0,
		//! Take the absolute value of the value.
		ABSOLUTE = 1,
		//! Take the square root of the value.
		SQUARE_ROOT = 2,
		//! Take the logarithm of the value.
		LOGARITHM = 3,
		//! Take the exponential of the value.
		EXPONENTIAL = 4
	};

	//! Evaluate the unary operation on a cell value.
	static bool Eval(CubeUnaryOp::Value op, const CubeCellValue &value, double &result);
};

//! Binary operations that can be applied to a data cube.
struct CubeBinaryOp {
	enum Value : uint8_t {
		//! Compare the values in the data cube for equality.
		EQUAL = 0,
		//! Compare the values in the data cube for inequality.
		NOT_EQUAL = 1,
		//! Compare if the values in the data cube are greater.
		GREATER = 2,
		//! Compare if the values in the data cube are less.
		LESS = 3,
		//! Compare if the values in the data cube are greater or equal.
		GREATER_EQUAL = 4,
		//! Compare if the values in the data cube are less or equal.
		LESS_EQUAL = 5,
		//! Add the values in the data cube.
		ADD = 6,
		//! Subtract the values in the data cube.
		SUBTRACT = 7,
		//! Multiply the values in the data cube.
		MULTIPLY = 8,
		//! Divide the values in the data cube.
		DIVIDE = 9,
		//! Take the power of the values in the data cube.
		POW = 10,
		//! Take the modulus of the values in the data cube.
		MOD = 11,
		//! Set the value in the data cube.
		SET = 12,
		//! Take the minimum of the values in the data cube.
		MIN = 13,
		//! Take the maximum of the values in the data cube.
		MAX = 14,
		//! Take the first non no-data value between the values in the data cube.
		OR = 15,
		//! Set the value in the data cube without any validity check.
		FILL = 16
	};

	//! Evaluate the binary operation on two values.
	static bool Eval(CubeBinaryOp::Value op, const CubeCellValue &a, const CubeCellValue &b, double &result);
};

} // namespace duckdb
