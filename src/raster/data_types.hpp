#pragma once

#include <cstdint>
#include <string>

namespace duckdb {

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
	static int64_t GetSizeBytes(const Value &data_type);

	//! Convert a DataType::Value to a string.
	static std::string ToString(const DataType::Value &data_type);
};

//! Data format of a BLOB databand column, e.g. raw bytes, compressed, etc.
struct DataFormat {
	enum Value : uint8_t { RAW = 0 };

	//! Convert a string to a DataFormat.
	static Value FromString(const std::string &format_str);

	//! Convert a DataFormat to a string.
	static std::string ToString(const DataFormat::Value &data_format);
};

} // namespace duckdb
