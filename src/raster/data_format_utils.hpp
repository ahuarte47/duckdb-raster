#pragma once

#include <cstdint>
#include "data_types.hpp"

namespace duckdb {

class MemoryStream;

class DataFormatUtils {
public:
	//! Compress the input stream using the specified format and write to the output stream.
	//! Returns the size of the compressed data.
	static size_t Compress(const char *input_ptr, size_t input_size, DataFormat::Value format, MemoryStream &output);

	//! Decompress from the input stream using the specified format and write to the output stream.
	//! Returns the size of the decompressed data.
	static size_t Decompress(const char *input_ptr, size_t input_size, DataFormat::Value format, MemoryStream &output);
};

} // namespace duckdb
