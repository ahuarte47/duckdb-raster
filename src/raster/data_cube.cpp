#include "data_cube.hpp"
#include "raster_types.hpp"

namespace duckdb {

DataCube::DataCube() : DataCube::DataCube(Allocator::DefaultAllocator()) {
}

DataCube::DataCube(Allocator &allocator) : allocator(allocator), header(), buffer(allocator) {
}

DataCube DataCube::EMPTY_CUBE() {
	DataCube empty_cube;
	empty_cube.SetHeader(empty_cube.GetHeader(), true);
	return empty_cube;
}

DataHeader DataCube::GetHeader() const {
	return header;
}

void DataCube::SetHeader(const DataHeader &header, bool init_buffer) {
	this->header = header;

	// Clear the buffer and write the header to it.
	if (init_buffer) {
		buffer.GrowCapacity(GetExpectedSizeBytes());
		buffer.Rewind();
		buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));
	}
}

MemoryStream &DataCube::GetBuffer() {
	return buffer;
}

int64_t DataCube::GetCubeSize() const {
	return static_cast<int64_t>(header.bands) * header.cols * header.rows;
}

int64_t DataCube::GetExpectedSizeBytes() const {
	int64_t cube_size = GetCubeSize();
	int64_t data_size = cube_size * DataType::GetSizeBytes(header.data_type);
	return sizeof(DataHeader) + data_size;
}

int64_t DataCube::LoadBlob(const Value &blob) {
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
	if (blob_size < sizeof(DataHeader)) {
		throw std::runtime_error("BLOB size is too small to contain a valid DataHeader");
	}

	MemoryStream blob_buffer(blob_data, blob_size);
	DataHeader blob_header = blob_buffer.Read<DataHeader>();
	if (blob_header.magic != DATA_BLOCK_HEADER_MAGIC) {
		throw std::runtime_error("BLOB does not contain a valid DataHeader (invalid magic code)");
	}

	// Set the header and the buffer of the data cube from the BLOB content.

	header = blob_header;

	buffer.Rewind();
	buffer.GrowCapacity(blob_size);
	buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));

	const idx_t num_values = static_cast<idx_t>(header.bands) * header.cols * header.rows;
	if (num_values > 0) {
		const idx_t data_size = blob_size - sizeof(DataHeader);
		buffer.WriteData(blob_data + sizeof(DataHeader), data_size);
	}
	return buffer.GetPosition();
}

Value DataCube::ToBlob() const {
	switch (header.data_format) {
	case DataFormat::RAW: {
		// Nothing to do for raw data, the buffer already contains the header and the raw tile data.
		Value blob = Value::BLOB(buffer.GetData(), GetExpectedSizeBytes());
		blob.Reinterpret(RasterTypes::DATACUBE());
		return blob;
	}
	default:
		throw std::runtime_error("Unsupported data format: " + std::to_string(header.data_format));
	}
}

int64_t DataCube::LoadArray(const Value &in_array, const DataHeader &in_header) {
	if (in_array.IsNull()) {
		throw std::runtime_error("Cannot convert NULL array to stream");
	}
	if (in_array.type().id() != LogicalTypeId::LIST) {
		throw std::runtime_error("Expected a LIST value, but got " + in_array.type().ToString());
	}

	const auto &children = ListValue::GetChildren(in_array);

	// Validate that the number of values in the input array matches the expected number of values.

	const std::size_t num_values = static_cast<size_t>(in_header.bands) * in_header.cols * in_header.rows;
	if (children.size() != num_values) {
		throw std::runtime_error(
		    "The number of values in the input array does not match the expected number of values.");
	}

	// Set the header and the buffer of the data cube from the ARRAY content.

	header = in_header;
	header.data_format = DataFormat::RAW;

	buffer.Rewind();
	buffer.GrowCapacity(GetExpectedSizeBytes());
	buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));

	if (children.empty()) {
		return buffer.GetPosition();
	}

	auto write_values = [&](auto make_value) {
		using T = decltype(make_value(children[0]));

		vector<T> raw;
		raw.reserve(children.size());

		for (const auto &value : children) {
			raw.push_back(make_value(value));
		}
		buffer.WriteData(const_data_ptr_cast(raw.data()), raw.size() * sizeof(T));
	};

	switch (header.data_type) {
	case DataType::UINT8:
		write_values([](const Value &v) { return v.GetValue<uint8_t>(); });
		break;
	case DataType::INT8:
		write_values([](const Value &v) { return v.GetValue<int8_t>(); });
		break;
	case DataType::UINT16:
		write_values([](const Value &v) { return v.GetValue<uint16_t>(); });
		break;
	case DataType::INT16:
		write_values([](const Value &v) { return v.GetValue<int16_t>(); });
		break;
	case DataType::UINT32:
		write_values([](const Value &v) { return v.GetValue<uint32_t>(); });
		break;
	case DataType::INT32:
		write_values([](const Value &v) { return v.GetValue<int32_t>(); });
		break;
	case DataType::UINT64:
		write_values([](const Value &v) { return v.GetValue<uint64_t>(); });
		break;
	case DataType::INT64:
		write_values([](const Value &v) { return v.GetValue<int64_t>(); });
		break;
	case DataType::FLOAT:
		write_values([](const Value &v) { return v.GetValue<float>(); });
		break;
	case DataType::DOUBLE:
		write_values([](const Value &v) { return v.GetValue<double>(); });
		break;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(header.data_type));
	}

	return buffer.GetPosition();
}

Value DataCube::ToArray(const LogicalType &output_type, bool filter_nodata) {
	const DataType::Value data_type = header.data_type;
	const double no_data = header.no_data;
	bool no_data_is_nan = std::isnan(no_data);
	vector<Value> data_values;

	// TODO:
	// Handle not RAW data formats when we support them.
	// For now we assume that the buffer contains the header followed by the raw tile data.
	//

	idx_t cube_size = GetCubeSize();
	if (cube_size > 0) {
		data_values.reserve(cube_size);

		auto read_values = [&](auto read_raw, auto make_value) {
			if (filter_nodata) {
				for (idx_t i = 0; i < cube_size; i++) {
					const auto raw_value = read_raw();

					if (no_data_is_nan && std::isnan(raw_value)) {
						continue;
					}
					if (static_cast<double>(raw_value) != no_data) {
						data_values.push_back(make_value(raw_value));
					}
				}
			} else {
				for (idx_t i = 0; i < cube_size; i++) {
					const auto raw_value = read_raw();
					data_values.push_back(make_value(raw_value));
				}
			}
		};

		const auto old_position = buffer.GetPosition();
		buffer.SetPosition(sizeof(DataHeader));

		switch (data_type) {
		case DataType::UINT8:
			read_values([&] { return buffer.Read<uint8_t>(); }, [](auto v) { return Value::UTINYINT(v); });
			break;
		case DataType::INT8:
			read_values([&] { return buffer.Read<int8_t>(); }, [](auto v) { return Value::TINYINT(v); });
			break;
		case DataType::UINT16:
			read_values([&] { return buffer.Read<uint16_t>(); }, [](auto v) { return Value::USMALLINT(v); });
			break;
		case DataType::INT16:
			read_values([&] { return buffer.Read<int16_t>(); }, [](auto v) { return Value::SMALLINT(v); });
			break;
		case DataType::UINT32:
			read_values([&] { return buffer.Read<uint32_t>(); }, [](auto v) { return Value::UINTEGER(v); });
			break;
		case DataType::INT32:
			read_values([&] { return buffer.Read<int32_t>(); }, [](auto v) { return Value::INTEGER(v); });
			break;
		case DataType::UINT64:
			read_values([&] { return buffer.Read<uint64_t>(); }, [](auto v) { return Value::UBIGINT(v); });
			break;
		case DataType::INT64:
			read_values([&] { return buffer.Read<int64_t>(); }, [](auto v) { return Value::BIGINT(v); });
			break;
		case DataType::FLOAT:
			read_values([&] { return buffer.Read<float>(); }, [](auto v) { return Value::FLOAT(v); });
			break;
		case DataType::DOUBLE:
			read_values([&] { return buffer.Read<double>(); }, [](auto v) { return Value::DOUBLE(v); });
			break;
		default: {
			buffer.SetPosition(old_position);
			throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
		}
		}

		buffer.SetPosition(old_position);
	}

	// Return a RT_ARRAY struct with the metadata and the data of the tile.
	Value data = Value::STRUCT({
	    {"data_type", Value::INTEGER(data_type)},
	    {"bands", Value::INTEGER(header.bands)},
	    {"cols", Value::INTEGER(header.cols)},
	    {"rows", Value::INTEGER(header.rows)},
	    {"no_data", Value::DOUBLE(header.no_data)},
	    {"values", Value::LIST(output_type, std::move(data_values))},
	});
	data.Reinterpret(RasterTypes::ARRAY(output_type));
	return data;
}

} // namespace duckdb
