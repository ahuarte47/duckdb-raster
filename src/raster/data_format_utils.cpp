#include "data_format_utils.hpp"

// DuckDB
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

// Compression libraries (Snappy, GZIP, ZSTD, LZ4)
#include "snappy.h"
#include "miniz_wrapper.hpp"
#include "zstd.h"
#include "lz4.hpp"

namespace duckdb {

size_t DataFormatUtils::Compress(const char *input_ptr, size_t input_size, DataFormat::Value format,
                                 MemoryStream &output) {
	output.Rewind();

	// No data to compress; write nothing and return 0.
	if (input_size == 0) {
		return 0;
	}

	// Compression based on the specified format.
	switch (format) {
	case DataFormat::Value::RAW: {
		output.WriteData(const_data_ptr_cast(input_ptr), input_size);
		return input_size;
	}
	case DataFormat::Value::SNAPPY: {
		size_t compressed_size = duckdb_snappy::MaxCompressedLength(input_size);
		output.GrowCapacity(compressed_size);

		duckdb_snappy::RawCompress(input_ptr, input_size, char_ptr_cast(output.GetData()), &compressed_size);
		output.SetPosition(compressed_size);
		return compressed_size;
	}
	case DataFormat::Value::GZIP: {
		size_t compressed_size = MiniZStream::MaxCompressedLength(input_size);
		output.GrowCapacity(compressed_size);

		MiniZStream s;
		s.Compress(input_ptr, input_size, char_ptr_cast(output.GetData()), &compressed_size);
		output.SetPosition(compressed_size);
		return compressed_size;
	}
	case DataFormat::Value::ZSTD: {
		size_t compressed_size = duckdb_zstd::ZSTD_compressBound(input_size);
		output.GrowCapacity(compressed_size);

		auto result = duckdb_zstd::ZSTD_compress(char_ptr_cast(output.GetData()), compressed_size, input_ptr,
		                                         input_size, duckdb_zstd::ZSTD_defaultCLevel());

		if (duckdb_zstd::ZSTD_isError(result)) {
			throw IOException("ZSTD compression failed: %s", duckdb_zstd::ZSTD_getErrorName(result));
		}
		output.SetPosition(result);
		return result;
	}
	case DataFormat::Value::LZ4_RAW: {
		// Prefix: 4 bytes storing the original uncompressed size (little-endian uint32).
		// Required because LZ4 raw streams do not carry the uncompressed size.
		constexpr size_t PREFIX_SIZE = sizeof(uint32_t);
		size_t bound = duckdb_lz4::LZ4_compressBound(UnsafeNumericCast<int32_t>(input_size));
		output.GrowCapacity(PREFIX_SIZE + bound);

		// Write original size prefix.
		auto *prefix = reinterpret_cast<uint8_t *>(output.GetData());
		uint32_t orig = UnsafeNumericCast<uint32_t>(input_size);
		prefix[0] = orig & 0xFF;
		prefix[1] = (orig >> 8) & 0xFF;
		prefix[2] = (orig >> 16) & 0xFF;
		prefix[3] = (orig >> 24) & 0xFF;

		int compressed_size =
		    duckdb_lz4::LZ4_compress_default(input_ptr, char_ptr_cast(output.GetData()) + PREFIX_SIZE,
		                                     UnsafeNumericCast<int32_t>(input_size), UnsafeNumericCast<int32_t>(bound));

		if (compressed_size <= 0) {
			throw IOException("LZ4 compression failed");
		}
		size_t total = PREFIX_SIZE + UnsafeNumericCast<size_t>(compressed_size);
		output.SetPosition(total);
		return total;
	}
	default:
		throw std::invalid_argument("Unsupported compression: " + std::to_string(static_cast<int>(format)));
	}
}

size_t DataFormatUtils::Decompress(const char *input_ptr, size_t input_size, DataFormat::Value format,
                                   MemoryStream &output) {
	output.Rewind();

	// No data to decompress; write nothing and return 0.
	if (input_size == 0) {
		return 0;
	}

	size_t decompressed_size = 0;

	// Decompress based on the specified format.
	switch (format) {
	case DataFormat::Value::RAW: {
		output.WriteData(const_data_ptr_cast(input_ptr), input_size);
		return input_size;
	}
	case DataFormat::Value::SNAPPY: {
		if (!duckdb_snappy::GetUncompressedLength(input_ptr, input_size, &decompressed_size)) {
			throw IOException("Failed to get uncompressed length for Snappy data");
		}
		output.GrowCapacity(decompressed_size);

		if (!duckdb_snappy::RawUncompress(input_ptr, input_size, char_ptr_cast(output.GetData()))) {
			throw IOException("Failed to decompress Snappy data");
		}
		output.SetPosition(decompressed_size);
		return decompressed_size;
	}
	case DataFormat::Value::GZIP: {
		if (input_size < MiniZStream::GZIP_FOOTER_SIZE) {
			throw IOException("GZIP decompression failed: input too small");
		}
		// Read ISIZE from the last 4 bytes of the GZIP footer (little-endian uint32).
		const auto *footer = reinterpret_cast<const uint8_t *>(input_ptr) + input_size - 4;
		decompressed_size = (uint32_t)footer[0] | ((uint32_t)footer[1] << 8) | ((uint32_t)footer[2] << 16) |
		                    ((uint32_t)footer[3] << 24);
		output.GrowCapacity(decompressed_size);

		MiniZStream s;
		s.Decompress(input_ptr, input_size, char_ptr_cast(output.GetData()), decompressed_size);
		output.SetPosition(decompressed_size);
		return decompressed_size;
	}
	case DataFormat::Value::ZSTD: {
		decompressed_size = duckdb_zstd::ZSTD_getFrameContentSize(input_ptr, input_size);
		if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
			throw IOException("ZSTD decompression failed: not a valid compressed frame");
		}
		if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
			throw IOException("ZSTD decompression failed: original size unknown, use streaming decompression");
		}
		output.GrowCapacity(decompressed_size);

		auto result =
		    duckdb_zstd::ZSTD_decompress(char_ptr_cast(output.GetData()), decompressed_size, input_ptr, input_size);

		if (duckdb_zstd::ZSTD_isError(result)) {
			throw IOException("ZSTD decompression failed: %s", duckdb_zstd::ZSTD_getErrorName(result));
		}
		output.SetPosition(result);
		return result;
	}
	case DataFormat::Value::LZ4_RAW: {
		constexpr size_t PREFIX_SIZE = sizeof(uint32_t);
		if (input_size < PREFIX_SIZE) {
			throw IOException("LZ4 decompression failed: input too small");
		}
		// Read original uncompressed size from the 4-byte prefix.
		const auto *prefix = reinterpret_cast<const uint8_t *>(input_ptr);
		uint32_t orig = (uint32_t)prefix[0] | ((uint32_t)prefix[1] << 8) | ((uint32_t)prefix[2] << 16) |
		                ((uint32_t)prefix[3] << 24);
		decompressed_size = orig;
		output.GrowCapacity(decompressed_size);

		int result = duckdb_lz4::LZ4_decompress_safe(input_ptr + PREFIX_SIZE, char_ptr_cast(output.GetData()),
		                                             UnsafeNumericCast<int32_t>(input_size - PREFIX_SIZE),
		                                             UnsafeNumericCast<int32_t>(decompressed_size));

		if (result < 0) {
			throw IOException("LZ4 decompression failed");
		}
		output.SetPosition(UnsafeNumericCast<size_t>(result));
		return UnsafeNumericCast<size_t>(result);
	}
	default:
		throw std::invalid_argument("Unsupported compression: " + std::to_string(static_cast<int>(format)));
	}
}

} // namespace duckdb
