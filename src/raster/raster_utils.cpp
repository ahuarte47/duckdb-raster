#include "raster_utils.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace duckdb {

//======================================================================================================================
// Raster utilities
//======================================================================================================================

std::string RasterUtils::GetLastGdalErrorMsg() {
	return std::string(CPLGetLastErrorMsg());
}

Point2D RasterUtils::RasterCoordToWorldCoord(const double matrix[6], const int32_t &col, const int32_t &row) {
	const double x = matrix[0] + matrix[1] * col + matrix[2] * row;
	const double y = matrix[3] + matrix[4] * col + matrix[5] * row;
	return Point2D(x, y);
}

int RasterUtils::GetSrid(const char *proj_def) {
	int srid = 0; // SRID_UNKNOWN

	if (proj_def) {
		OGRSpatialReference spatial_ref;
		spatial_ref.SetAxisMappingStrategy(OAMS_TRADITIONAL_GIS_ORDER);

		if (spatial_ref.importFromWkt(proj_def) == OGRERR_NONE && spatial_ref.AutoIdentifyEPSG() == OGRERR_NONE) {
			const char *code = spatial_ref.GetAuthorityCode(nullptr);
			if (code) {
				srid = atoi(code);
			}
		}
	}
	return srid;
}

Value RasterUtils::TileAsBlob(const TileHeader &header, MemoryStream &data_buffer, const idx_t &data_length) {
	switch (header.compression) {
	case CompressionAlg::NONE:
		// Nothing to do for uncompressed tiles, the buffer already contains the header and the raw tile data.
		return Value::BLOB(data_buffer.GetData(), data_length);
	default:
		throw std::runtime_error("Unsupported compression: " + CompressionAlg::ToString(header.compression));
	}
}

Value RasterUtils::BlobAsArray(const Value &blob, const LogicalType &array_type, const bool &filter_nodata) {
	// Validate the input BLOB value.
	if (blob.IsNull()) {
		throw std::runtime_error("Cannot convert NULL blob to array");
	}
	if (blob.type().id() != LogicalTypeId::BLOB) {
		throw std::runtime_error("Expected a BLOB value, but got " + blob.type().ToString());
	}

	// Validate if the BLOB contains the expected metadata and data structure.
	const auto &blob_str = StringValue::Get(blob);
	const auto blob_data = reinterpret_cast<data_ptr_t>(const_cast<char *>(blob_str.data()));
	const auto blob_size = blob_str.size();
	if (blob_size < sizeof(TileHeader)) {
		throw std::runtime_error("BLOB size is too small to contain a valid TileHeader");
	}

	MemoryStream data_stream(blob_data, blob_size);
	TileHeader header = data_stream.Read<TileHeader>();
	if (header.magic != RASTER_BLOCK_HEADER_MAGIC) {
		throw std::runtime_error("BLOB does not contain a valid TileHeader (invalid magic code)");
	}

	const RasterDataType::Value data_type = header.data_type;
	const double no_data = header.no_data;
	bool no_data_is_nan = std::isnan(no_data);
	vector<Value> data_values;

	// TODO:
	// Handle compressed tiles when we add support for compression algorithms,
	// currently we only support uncompressed tiles.
	//

	// Read the tile data from the stream and convert it to an array of the appropriate type.
	idx_t num_values = static_cast<idx_t>(header.bands) * header.cols * header.rows;
	if (num_values > 0) {
		data_values.reserve(num_values);

		auto read_values = [&](auto read_raw, auto make_value) {
			if (filter_nodata) {
				for (idx_t i = 0; i < num_values; i++) {
					const auto raw_value = read_raw();

					if (no_data_is_nan && std::isnan(raw_value)) {
						continue;
					}
					if (static_cast<double>(raw_value) != no_data) {
						data_values.push_back(make_value(raw_value));
					}
				}
			} else {
				for (idx_t i = 0; i < num_values; i++) {
					const auto raw_value = read_raw();
					data_values.push_back(make_value(raw_value));
				}
			}
		};

		switch (data_type) {
		case RasterDataType::UINT8:
			read_values([&] { return data_stream.Read<uint8_t>(); }, [](auto v) { return Value::UTINYINT(v); });
			break;
		case RasterDataType::INT8:
			read_values([&] { return data_stream.Read<int8_t>(); }, [](auto v) { return Value::TINYINT(v); });
			break;
		case RasterDataType::UINT16:
			read_values([&] { return data_stream.Read<uint16_t>(); }, [](auto v) { return Value::USMALLINT(v); });
			break;
		case RasterDataType::INT16:
			read_values([&] { return data_stream.Read<int16_t>(); }, [](auto v) { return Value::SMALLINT(v); });
			break;
		case RasterDataType::UINT32:
			read_values([&] { return data_stream.Read<uint32_t>(); }, [](auto v) { return Value::UINTEGER(v); });
			break;
		case RasterDataType::INT32:
			read_values([&] { return data_stream.Read<int32_t>(); }, [](auto v) { return Value::INTEGER(v); });
			break;
		case RasterDataType::UINT64:
			read_values([&] { return data_stream.Read<uint64_t>(); }, [](auto v) { return Value::UBIGINT(v); });
			break;
		case RasterDataType::INT64:
			read_values([&] { return data_stream.Read<int64_t>(); }, [](auto v) { return Value::BIGINT(v); });
			break;
		case RasterDataType::FLOAT:
			read_values([&] { return data_stream.Read<float>(); }, [](auto v) { return Value::FLOAT(v); });
			break;
		case RasterDataType::DOUBLE:
			read_values([&] { return data_stream.Read<double>(); }, [](auto v) { return Value::DOUBLE(v); });
			break;
		default:
			throw std::runtime_error("Unsupported RasterDataType: '" + RasterDataType::ToString(data_type) + "'");
		}
	}

	// Return a RT_ARRAY struct with the metadata and the data of the tile.
	Value data = Value::STRUCT({
	    {"data_type", Value::INTEGER(data_type)},
	    {"bands", Value::INTEGER(header.bands)},
	    {"cols", Value::INTEGER(header.cols)},
	    {"rows", Value::INTEGER(header.rows)},
	    {"no_data", Value::DOUBLE(header.no_data)},
	    {"values", Value::LIST(array_type, std::move(data_values))},
	});
	return data;
}

std::size_t RasterUtils::BlobAsStream(const Value &blob, TileHeader &data_header, MemoryStream &data_buffer) {
	// Validate the input BLOB value.
	if (blob.IsNull()) {
		throw std::runtime_error("Cannot convert NULL blob to array");
	}
	if (blob.type().id() != LogicalTypeId::BLOB) {
		throw std::runtime_error("Expected a BLOB value, but got " + blob.type().ToString());
	}

	// Validate if the BLOB contains the expected metadata and data structure.
	const auto &blob_str = StringValue::Get(blob);
	const auto blob_data = reinterpret_cast<data_ptr_t>(const_cast<char *>(blob_str.data()));
	const auto blob_size = blob_str.size();
	if (blob_size < sizeof(TileHeader)) {
		throw std::runtime_error("BLOB size is too small to contain a valid TileHeader");
	}

	MemoryStream blob_stream(blob_data, blob_size);
	TileHeader header = blob_stream.Read<TileHeader>();
	if (header.magic != RASTER_BLOCK_HEADER_MAGIC) {
		throw std::runtime_error("BLOB does not contain a valid TileHeader (invalid magic code)");
	}

	// Get the tile data.

	data_header = header;

	// TODO:
	// Handle compressed tiles when we add support for compression algorithms,
	// currently we only support uncompressed tiles.
	//

	const idx_t num_values = static_cast<idx_t>(header.bands) * header.cols * header.rows;
	if (num_values > 0) {
		const idx_t data_size = blob_size - sizeof(TileHeader);
		data_buffer.WriteData(blob_data + sizeof(TileHeader), data_size);
		return data_size;
	}
	return 0;
}

std::size_t RasterUtils::ArrayAsStream(const Value &in_array, const TileHeader &in_header, MemoryStream &data_buffer) {
	// Validate the input ARRAY value.
	if (in_array.IsNull()) {
		throw std::runtime_error("Cannot convert NULL array to stream");
	}
	if (in_array.type().id() != LogicalTypeId::LIST) {
		throw std::runtime_error("Expected a LIST value, but got " + in_array.type().ToString());
	}

	// TODO:
	// Handle compressed tiles when we add support for compression algorithms,
	// currently we only support uncompressed tiles.
	//

	idx_t start_position = data_buffer.GetPosition();
	const auto &children = ListValue::GetChildren(in_array);

	const std::size_t num_values = static_cast<size_t>(in_header.bands) * in_header.cols * in_header.rows;
	if (children.size() != num_values) {
		throw std::runtime_error("The number of values in the input array does not match the expected number of values "
		                         "based on the tile dimensions and bands");
	}

	// Write the tile header to the stream.

	data_buffer.Write<TileHeader>(in_header);

	// Write the tile data to the stream.

	if (children.empty()) {
		return data_buffer.GetPosition() - start_position;
	}

	auto write_values = [&](auto make_value) {
		using T = decltype(make_value(children[0]));

		vector<T> raw;
		raw.reserve(children.size());

		for (const auto &value : children) {
			raw.push_back(make_value(value));
		}
		data_buffer.WriteData(const_data_ptr_cast(raw.data()), raw.size() * sizeof(T));
	};

	switch (in_header.data_type) {
	case RasterDataType::UINT8:
		write_values([](const Value &v) { return v.GetValue<uint8_t>(); });
		break;
	case RasterDataType::INT8:
		write_values([](const Value &v) { return v.GetValue<int8_t>(); });
		break;
	case RasterDataType::UINT16:
		write_values([](const Value &v) { return v.GetValue<uint16_t>(); });
		break;
	case RasterDataType::INT16:
		write_values([](const Value &v) { return v.GetValue<int16_t>(); });
		break;
	case RasterDataType::UINT32:
		write_values([](const Value &v) { return v.GetValue<uint32_t>(); });
		break;
	case RasterDataType::INT32:
		write_values([](const Value &v) { return v.GetValue<int32_t>(); });
		break;
	case RasterDataType::UINT64:
		write_values([](const Value &v) { return v.GetValue<uint64_t>(); });
		break;
	case RasterDataType::INT64:
		write_values([](const Value &v) { return v.GetValue<int64_t>(); });
		break;
	case RasterDataType::FLOAT:
		write_values([](const Value &v) { return v.GetValue<float>(); });
		break;
	case RasterDataType::DOUBLE:
		write_values([](const Value &v) { return v.GetValue<double>(); });
		break;
	default:
		throw std::runtime_error("Unsupported RasterDataType: '" + RasterDataType::ToString(in_header.data_type) + "'");
	}
	return data_buffer.GetPosition() - start_position;
}

} // namespace duckdb
