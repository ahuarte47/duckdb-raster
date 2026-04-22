#pragma once

#include "data_types.hpp"

// DuckDB
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! Magic code to identify a BLOB as a raster block (ASCII "RS" = 0x5253).
#define DATA_BLOCK_HEADER_MAGIC ((uint16_t)0x5253)

//! Header describing the tile data stored in the BLOB column.
class DataHeader {
public:
	uint16_t magic;                // Magic code to identify a BLOB as a raster block (DATA_BLOCK_HEADER_MAGIC)
	DataFormat::Value data_format; // Format used for the tile data (Raw, GZip, etc)
	DataType::Value data_type;     // DataType of the tile data (Byte, UInt16, Int32, etc)
	int32_t bands;                 // Number of bands or layers in the data buffer
	int32_t cols;                  // Number of columns in the data buffer
	int32_t rows;                  // Number of rows in the data buffer
	double no_data;                // NoData value for the tile (To consider when applying algebra operations)

	DataHeader()
	    : magic(DATA_BLOCK_HEADER_MAGIC), data_format(DataFormat::RAW), data_type(DataType::UINT8), bands(0), cols(0),
	      rows(0), no_data(NumericLimits<double>::Minimum()) {
	}
	DataHeader(DataFormat::Value data_format, DataType::Value data_type, int32_t bands, int32_t cols, int32_t rows,
	           double no_data)
	    : magic(DATA_BLOCK_HEADER_MAGIC), data_format(data_format), data_type(data_type), bands(bands), cols(cols),
	      rows(rows), no_data(no_data) {
	}
};

//! Unary operations that can be applied to a data cube.
enum class CubeUnaryOp : uint8_t {
	//! Negate the values in the data cube (multiply by -1).
	NEGATE = 0,
	//! Take the absolute value of the values in the data cube.
	ABSOLUTE = 1,
	//! Take the square root of the values in the data cube.
	SQUARE_ROOT = 2,
	//! Take the logarithm of the values in the data cube.
	LOGARITHM = 3,
	//! Take the exponential of the values in the data cube.
	EXPONENTIAL = 4
};

//! Binary operations that can be applied to a data cube.
enum class CubeBinaryOp : uint8_t {
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
};

//! A N-dimensional data cube containing multiple raster bands, stored in a BLOB column.
class DataCube {
public:
	DataCube();
	DataCube(Allocator &allocator);

	//! Get an empty DataCube instance.
	static DataCube EMPTY_CUBE(DataType::Value data_type = DataType::UINT8);

private:
	//! The allocator used for the data buffer.
	Allocator &allocator;

	//! The header of the data cube, containing metadata about the tile.
	DataHeader header;
	//! The raw data of the tile, stored as a byte stream (BLOB). The format is determined by the header's data_format.
	MemoryStream buffer;

public:
	//! Get the header of the data cube.
	DataHeader GetHeader() const;
	//! Set the header of the data cube.
	void SetHeader(const DataHeader &header, bool init_buffer = true);

	// Get the data buffer of the data cube, The buffer must contain the header followed by the tile data.
	const MemoryStream &GetBuffer() const;
	// Get the data buffer of the data cube, The buffer must contain the header followed by the tile data.
	MemoryStream &GetBuffer();

	//! Get the total dimensions size of the data cube.
	int64_t GetCubeSize() const;
	// Get the full size of the data cube in bytes, including the header and the data buffer.
	int64_t GetExpectedSizeBytes() const;

	//! Load the data cube from a BLOB value, parsing the header and the data buffer.
	void LoadBlob(const Value &blob);
	//! Convert the data cube to a BLOB value.
	Value ToBlob() const;

	//! Load the data cube from an ARRAY value, parsing the header and the data values in the array.
	void LoadArray(const Value &in_array, const DataHeader &in_header);
	//! Convert the data cube to an ARRAY value, ignoring no-data values if specified.
	Value ToArray(const LogicalType &output_type, bool filter_nodata = false);

	//! Get the Value at the specified Cube coordinates (band, column, row).
	template <typename T>
	T GetValue(uint32_t band, uint32_t col, uint32_t row) const;
	//! Get the Value at the specified linear index.
	template <typename T>
	T GetValue(idx_t index) const;

private:
	//! Check if a value is a no-data value.
	static bool IsNoDataValue(double value, double no_data);
	//! Check if a value is valid to be used in computations.
	static bool IsValidValue(double value, double no_data);

	//! Read a value from the data buffer at the specified index.
	template <typename T>
	static T ReadValueAs(DataType::Value data_type, const data_ptr_t data_ptr, idx_t value_index);

public:
	//! Change the data format of the data cube, converting the data buffer accordingly.
	void ChangeFormat(const DataFormat::Value &new_format, DataCube &r) const;
	//! Change the data type of the data cube, converting the data buffer accordingly.
	void ChangeType(const DataType::Value &new_data_type, DataCube &r) const;

	//! Apply a unary operation to the data cube, storing the result in another data cube.
	static void Apply(CubeUnaryOp op, const DataCube &a, DataCube &r);
	//! Apply a binary operation to two data cubes, storing the result in a third data cube.
	static void Apply(CubeBinaryOp op, const DataCube &a, const DataCube &b, DataCube &r);
	//! Apply a binary operation to a data cube and a scalar value, storing the result in a third data cube.
	static void Apply(CubeBinaryOp op, const DataCube &a, const double &b, DataCube &r);

	//! Add the values in the data cube.
	DataCube operator+(const DataCube &other) const;
	//! Add a scalar value to the values in the data cube.
	DataCube operator+(const double &other) const;
	//! Subtract the values in the data cube.
	DataCube operator-(const DataCube &other) const;
	//! Subtract a scalar value from the values in the data cube.
	DataCube operator-(const double &other) const;
	//! Multiply the values in the data cube.
	DataCube operator*(const DataCube &other) const;
	//! Multiply the values in the data cube by a scalar value.
	DataCube operator*(const double &other) const;
	//! Divide the values in the data cube.
	DataCube operator/(const DataCube &other) const;
	//! Divide the values in the data cube by a scalar value.
	DataCube operator/(const double &other) const;
};

} // namespace duckdb
