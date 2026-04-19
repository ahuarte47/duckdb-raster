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

//! A N-dimensional data cube containing multiple raster bands, stored in a BLOB column.
class DataCube {
public:
	DataCube();
	DataCube(Allocator &allocator);

	//! Get an empty DataCube instance.
	static DataCube EMPTY_CUBE();

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
	MemoryStream &GetBuffer();

	//! Get the total dimensions size of the data cube.
	int64_t GetCubeSize() const;
	// Get the full size of the data cube in bytes, including the header and the data buffer.
	int64_t GetExpectedSizeBytes() const;

	//! Load the data cube from a BLOB value, parsing the header and the data buffer.
	int64_t LoadBlob(const Value &blob);
	//! Convert the data cube to a BLOB value.
	Value ToBlob() const;

	//! Load the data cube from an ARRAY value, parsing the header and the data values in the array.
	int64_t LoadArray(const Value &in_array, const DataHeader &in_header);
	//! Convert the data cube to an ARRAY value, ignoring no-data values if specified.
	Value ToArray(const LogicalType &output_type, bool filter_nodata = false);
};

} // namespace duckdb
