#pragma once

#include "data_types.hpp"

// DuckDB
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

//! Callback type for custom unary cell operations.
using CubeUnaryCellFunc = std::function<bool(const CubeCellValue &, double &)>;

//! Callback type for custom binary cell operations.
using CubeBinaryCellFunc = std::function<bool(const CubeCellValue &, const CubeCellValue &, double &)>;

//! Callback type for generic cell operations.
using CubeCellFunc = std::function<void(const CubeCellValue &)>;

//! An N-dimensional data cube containing one or more raster bands, stored in a BLOB column.
class DataCube {
public:
	DataCube();
	DataCube(Allocator &allocator);

	//! Get an empty DataCube instance.
	static DataCube EMPTY_CUBE(DataType::Value data_type = DataType::Value::UINT8);

private:
	//! The allocator used for the data buffer.
	Allocator &allocator;

	//! The header of the data cube, containing metadata about the tile.
	DataHeader header;

	//! The raw data of the tile, stored as a byte stream (BLOB).
	//! Format is determined by the header's data_format.
	MemoryStream data_buffer;

	//! A temporary buffer used for intermediate computations.
	MemoryStream temp_buffer;

public:
	//! Get the header of the data cube.
	DataHeader GetHeader() const;
	//! Set the header of the data cube.
	void SetHeader(const DataHeader &header, bool init_buffer = true);

	//! Get the data buffer (header followed by raw pixel data) as a read-only stream.
	const MemoryStream &GetBuffer() const;
	//! Get the data buffer (header followed by raw pixel data) as a writable stream.
	MemoryStream &GetBuffer();

	//! Get the total dimensions size of the data cube.
	int64_t GetCubeSize() const;
	//! Get the full size of the data cube in bytes, including the header and the data buffer.
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
	T GetValue(uint32_t band, uint32_t col, uint32_t row);
	//! Get the Value at the specified linear index.
	template <typename T>
	T GetValue(idx_t index);

	//! Returns true if the cube has no cells, or if every cell contains the nodata value.
	bool IsNullOrEmpty();

private:
	//! Read a value from the data buffer at the specified index.
	template <typename T>
	static T ReadValueAs(DataType::Value data_type, const data_ptr_t data_ptr, idx_t value_index);

public:
	//! Change the data format of the data cube, converting the data buffer accordingly.
	void ChangeFormat(const DataFormat::Value &new_format);
	//! Change the data type of the data cube, converting the data buffer accordingly.
	void ChangeType(const DataType::Value &new_data_type);

	//! Ensure that the data cube is in RAW format, converting it if necessary.
	//! This is required to apply algebra operations on the data cube.
	void EnsureRaw();

	//! Apply an unary cell function to every cell of `a`, writing results into `r`.
	static void Apply(const CubeUnaryCellFunc &func, DataCube &a, DataCube &r);
	//! Apply a binary cell function to every corresponding cell of `a` and `b`, writing results into `r`.
	static void Apply(const CubeBinaryCellFunc &func, DataCube &a, DataCube &b, DataCube &r);
	//! Apply a binary cell function to every cell of `a` paired with the scalar `b`, writing results into `r`.
	static void Apply(const CubeBinaryCellFunc &func, DataCube &a, const double &b, DataCube &r);
	//! Apply a generic cell function to every cell of `a`.
	static void Apply(const CubeCellFunc &func, DataCube &a);

	//! Add the values in the data cube.
	DataCube operator+(DataCube &other);
	//! Add a scalar value to the values in the data cube.
	DataCube operator+(double other);
	//! Subtract the values in the data cube.
	DataCube operator-(DataCube &other);
	//! Subtract a scalar value from the values in the data cube.
	DataCube operator-(double other);
	//! Multiply the values in the data cube.
	DataCube operator*(DataCube &other);
	//! Multiply the values in the data cube by a scalar value.
	DataCube operator*(double other);
	//! Divide the values in the data cube.
	DataCube operator/(DataCube &other);
	//! Divide the values in the data cube by a scalar value.
	DataCube operator/(double other);
};

} // namespace duckdb
