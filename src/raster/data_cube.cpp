#include "data_cube.hpp"
#include "raster_types.hpp"

namespace duckdb {

DataCube::DataCube() : DataCube::DataCube(Allocator::DefaultAllocator()) {
}

DataCube::DataCube(Allocator &allocator) : allocator(allocator), header(), buffer(allocator) {
}

DataCube DataCube::EMPTY_CUBE(DataType::Value data_type) {
	DataCube empty_cube;
	empty_cube.header.data_type = data_type;
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
		buffer.Rewind();
		buffer.GrowCapacity(GetExpectedSizeBytes());
		buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));
	}
}

const MemoryStream &DataCube::GetBuffer() const {
	return buffer;
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

void DataCube::LoadBlob(const Value &blob) {
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

	const idx_t cube_size = GetCubeSize();
	if (cube_size > 0) {
		const idx_t data_size = blob_size - sizeof(DataHeader);
		buffer.WriteData(blob_data + sizeof(DataHeader), data_size);
	}
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

void DataCube::LoadArray(const Value &in_array, const DataHeader &in_header) {
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
		return;
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
}

Value DataCube::ToArray(const LogicalType &output_type, bool filter_nodata) {
	if (header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Converting to array is only supported for RAW data format");
	}

	const DataType::Value data_type = header.data_type;
	const double no_data = header.no_data;
	vector<Value> data_values;

	idx_t cube_size = GetCubeSize();
	if (cube_size > 0) {
		data_values.reserve(cube_size);

		auto read_values = [&](auto read_raw, auto make_value) {
			if (filter_nodata) {
				for (idx_t i = 0; i < cube_size; i++) {
					const auto raw_value = read_raw();

					if (!DataCube::IsNoDataValue(static_cast<double>(raw_value), no_data)) {
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

template <typename T>
T DataCube::GetValue(uint32_t band, uint32_t col, uint32_t row) const {
	if (header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Getting values is only supported for RAW data format");
	}
	if (band >= header.bands) {
		throw std::out_of_range("Band index is out of bounds");
	}
	if (col >= header.cols) {
		throw std::out_of_range("Column index is out of bounds");
	}
	if (row >= header.rows) {
		throw std::out_of_range("Row index is out of bounds");
	}

	const idx_t index = static_cast<idx_t>(band) * header.cols * header.rows + static_cast<idx_t>(row) * header.cols +
	                    static_cast<idx_t>(col);

	return GetValue<T>(index);
}

template <typename T>
T DataCube::GetValue(idx_t index) const {
	if (header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Getting values is only supported for RAW data format");
	}
	if (index >= GetCubeSize()) {
		throw std::out_of_range("Index is out of bounds");
	}

	const idx_t byte_offset = sizeof(DataHeader) + (index * DataType::GetSizeBytes(header.data_type));
	const auto value_ptr = buffer.GetData() + byte_offset;

	return *reinterpret_cast<const T *>(value_ptr);
}

bool DataCube::IsNoDataValue(double value, double no_data) {
	return std::isnan(no_data) ? std::isnan(value) : value == no_data;
}

bool DataCube::IsValidValue(double value, double no_data) {
	return !std::isnan(value) && !std::isinf(value) && value != no_data;
}

template <typename T>
T DataCube::ReadValueAs(DataType::Value data_type, const data_ptr_t data_ptr, idx_t value_index) {
	switch (data_type) {
	case DataType::UINT8:
		return static_cast<T>(*reinterpret_cast<const uint8_t *>(data_ptr + value_index * sizeof(uint8_t)));
	case DataType::INT8:
		return static_cast<T>(*reinterpret_cast<const int8_t *>(data_ptr + value_index * sizeof(int8_t)));
	case DataType::UINT16:
		return static_cast<T>(*reinterpret_cast<const uint16_t *>(data_ptr + value_index * sizeof(uint16_t)));
	case DataType::INT16:
		return static_cast<T>(*reinterpret_cast<const int16_t *>(data_ptr + value_index * sizeof(int16_t)));
	case DataType::UINT32:
		return static_cast<T>(*reinterpret_cast<const uint32_t *>(data_ptr + value_index * sizeof(uint32_t)));
	case DataType::INT32:
		return static_cast<T>(*reinterpret_cast<const int32_t *>(data_ptr + value_index * sizeof(int32_t)));
	case DataType::UINT64:
		return static_cast<T>(*reinterpret_cast<const uint64_t *>(data_ptr + value_index * sizeof(uint64_t)));
	case DataType::INT64:
		return static_cast<T>(*reinterpret_cast<const int64_t *>(data_ptr + value_index * sizeof(int64_t)));
	case DataType::FLOAT:
		return static_cast<T>(*reinterpret_cast<const float *>(data_ptr + value_index * sizeof(float)));
	case DataType::DOUBLE:
		return static_cast<T>(*reinterpret_cast<const double *>(data_ptr + value_index * sizeof(double)));
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
	}
}

void DataCube::ChangeFormat(const DataFormat::Value &new_format, DataCube &r) const {
	if (header.data_format == new_format) {
		r.header = header;
		r.buffer.Rewind();
		r.buffer.GrowCapacity(buffer.GetCapacity());
		r.buffer.WriteData(buffer.GetData(), buffer.GetCapacity());
		r.buffer.SetPosition(buffer.GetPosition());
		return;
	}
	throw std::runtime_error("Data format conversion is not implemented yet");
}

void DataCube::ChangeType(const DataType::Value &new_data_type, DataCube &r) const {
	if (header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Data type conversion is only supported for RAW data format");
	}
	if (header.data_type == new_data_type) {
		r.header = header;
		r.buffer.Rewind();
		r.buffer.GrowCapacity(buffer.GetCapacity());
		r.buffer.WriteData(buffer.GetData(), buffer.GetCapacity());
		r.buffer.SetPosition(buffer.GetPosition());
		return;
	}

	r.header = header;
	r.header.data_format = DataFormat::RAW;
	r.header.data_type = new_data_type;
	r.buffer.Rewind();
	r.buffer.GrowCapacity(r.GetExpectedSizeBytes());
	r.buffer.WriteData(const_data_ptr_cast(&r.header), sizeof(DataHeader));

	const data_ptr_t source_data_ptr = buffer.GetData() + sizeof(DataHeader);
	data_ptr_t r_data_ptr = r.buffer.GetData() + sizeof(DataHeader);
	const idx_t cube_size = GetCubeSize();
	const DataType::Value src_type = header.data_type;

	auto write_as = [&](auto *typed_ptr) {
		using T = std::remove_pointer_t<decltype(typed_ptr)>;
		constexpr double t_min = static_cast<double>(std::numeric_limits<T>::lowest());
		constexpr double t_max = static_cast<double>(std::numeric_limits<T>::max());

		for (idx_t i = 0; i < cube_size; i++) {
			const double value = ReadValueAs<double>(src_type, source_data_ptr, i);
			typed_ptr[i] = static_cast<T>(ClampValue<double>(value, t_min, t_max));
		}
	};

	switch (new_data_type) {
	case DataType::UINT8:
		write_as(reinterpret_cast<uint8_t *>(r_data_ptr));
		break;
	case DataType::INT8:
		write_as(reinterpret_cast<int8_t *>(r_data_ptr));
		break;
	case DataType::UINT16:
		write_as(reinterpret_cast<uint16_t *>(r_data_ptr));
		break;
	case DataType::INT16:
		write_as(reinterpret_cast<int16_t *>(r_data_ptr));
		break;
	case DataType::UINT32:
		write_as(reinterpret_cast<uint32_t *>(r_data_ptr));
		break;
	case DataType::INT32:
		write_as(reinterpret_cast<int32_t *>(r_data_ptr));
		break;
	case DataType::UINT64:
		write_as(reinterpret_cast<uint64_t *>(r_data_ptr));
		break;
	case DataType::INT64:
		write_as(reinterpret_cast<int64_t *>(r_data_ptr));
		break;
	case DataType::FLOAT:
		write_as(reinterpret_cast<float *>(r_data_ptr));
		break;
	case DataType::DOUBLE:
		write_as(reinterpret_cast<double *>(r_data_ptr));
		break;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(new_data_type));
	}
}

void DataCube::Apply(CubeUnaryOp op, const DataCube &a, DataCube &r) {
	if (a.header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Applying unary operations is only supported for RAW data format");
	}

	DataHeader r_header = a.header;
	r_header.data_type = DataType::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.buffer.GetData() + sizeof(DataHeader));
	const double a_no_data = a.header.no_data;
	const DataType::Value a_data_type = a.header.data_type;
	const idx_t cube_size = a.GetCubeSize();

	auto apply_op = [op](double v) -> double {
		switch (op) {
		case CubeUnaryOp::NEGATE:
			return -v;
		case CubeUnaryOp::ABSOLUTE:
			return std::abs(v);
		case CubeUnaryOp::SQUARE_ROOT:
			return std::sqrt(v);
		case CubeUnaryOp::LOGARITHM:
			return std::log(v);
		case CubeUnaryOp::EXPONENTIAL:
			return std::exp(v);
		default:
			throw std::runtime_error("Unsupported operation: " + std::to_string(static_cast<uint8_t>(op)));
		}
	};

	for (idx_t i = 0; i < cube_size; i++) {
		const double value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);

		if (DataCube::IsValidValue(value, a_no_data)) {
			*r_data_ptr = apply_op(value);
		} else {
			*r_data_ptr = value;
		}
		r_data_ptr++;
	}
}

void DataCube::Apply(CubeBinaryOp op, const DataCube &a, const DataCube &b, DataCube &r) {
	if (a.header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Applying binary operations is only supported for RAW data format");
	}
	if (b.header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Applying binary operations is only supported for RAW data format");
	}
	if (a.header.bands != b.header.bands) {
		throw std::runtime_error("Input data cubes must have the same number of bands");
	}
	if (a.header.cols != b.header.cols) {
		throw std::runtime_error("Input data cubes must have the same number of columns");
	}
	if (a.header.rows != b.header.rows) {
		throw std::runtime_error("Input data cubes must have the same number of rows");
	}

	DataHeader r_header = a.header;
	r_header.data_type = DataType::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.buffer.GetData() + sizeof(DataHeader);
	data_ptr_t b_data_ptr = b.buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.buffer.GetData() + sizeof(DataHeader));
	const double a_no_data = a.header.no_data;
	const double b_no_data = b.header.no_data;
	const DataType::Value a_data_type = a.header.data_type;
	const DataType::Value b_data_type = b.header.data_type;
	const idx_t cube_size = a.GetCubeSize();

	auto apply_op = [op](double a, double b) -> double {
		switch (op) {
		case CubeBinaryOp::EQUAL:
			return (a == b) ? 1.0 : 0.0;
		case CubeBinaryOp::NOT_EQUAL:
			return (a != b) ? 1.0 : 0.0;
		case CubeBinaryOp::GREATER:
			return (a > b) ? 1.0 : 0.0;
		case CubeBinaryOp::LESS:
			return (a < b) ? 1.0 : 0.0;
		case CubeBinaryOp::GREATER_EQUAL:
			return (a >= b) ? 1.0 : 0.0;
		case CubeBinaryOp::LESS_EQUAL:
			return (a <= b) ? 1.0 : 0.0;
		case CubeBinaryOp::ADD:
			return a + b;
		case CubeBinaryOp::SUBTRACT:
			return a - b;
		case CubeBinaryOp::MULTIPLY:
			return a * b;
		case CubeBinaryOp::DIVIDE:
			return (b != 0.0) ? a / b : a;
		case CubeBinaryOp::POW:
			return std::pow(a, b);
		case CubeBinaryOp::MOD:
			return std::fmod(a, b);
		default:
			throw std::runtime_error("Unsupported operation: " + std::to_string(static_cast<uint8_t>(op)));
		}
	};

	for (idx_t i = 0; i < cube_size; i++) {
		const double a_value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);
		const double b_value = DataCube::ReadValueAs<double>(b_data_type, b_data_ptr, i);

		if (DataCube::IsValidValue(a_value, a_no_data) && DataCube::IsValidValue(b_value, b_no_data)) {
			*r_data_ptr = apply_op(a_value, b_value);
		} else {
			*r_data_ptr = a_value;
		}
		r_data_ptr++;
	}
}

void DataCube::Apply(CubeBinaryOp op, const DataCube &a, const double &b, DataCube &r) {
	if (a.header.data_format != DataFormat::RAW) {
		throw std::runtime_error("Applying binary operations is only supported for RAW data format");
	}
	if (op == CubeBinaryOp::DIVIDE && b == 0) {
		throw std::runtime_error("Division by zero is not allowed");
	}

	DataHeader r_header = a.header;
	r_header.data_type = DataType::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.buffer.GetData() + sizeof(DataHeader));
	const double a_no_data = a.header.no_data;
	const DataType::Value a_data_type = a.header.data_type;
	const idx_t cube_size = a.GetCubeSize();

	auto apply_op = [op](double a, double b) -> double {
		switch (op) {
		case CubeBinaryOp::EQUAL:
			return (a == b) ? 1.0 : 0.0;
		case CubeBinaryOp::NOT_EQUAL:
			return (a != b) ? 1.0 : 0.0;
		case CubeBinaryOp::GREATER:
			return (a > b) ? 1.0 : 0.0;
		case CubeBinaryOp::LESS:
			return (a < b) ? 1.0 : 0.0;
		case CubeBinaryOp::GREATER_EQUAL:
			return (a >= b) ? 1.0 : 0.0;
		case CubeBinaryOp::LESS_EQUAL:
			return (a <= b) ? 1.0 : 0.0;
		case CubeBinaryOp::ADD:
			return a + b;
		case CubeBinaryOp::SUBTRACT:
			return a - b;
		case CubeBinaryOp::MULTIPLY:
			return a * b;
		case CubeBinaryOp::DIVIDE:
			return a / b;
		case CubeBinaryOp::POW:
			return std::pow(a, b);
		case CubeBinaryOp::MOD:
			return std::fmod(a, b);
		default:
			throw std::runtime_error("Unsupported operation: " + std::to_string(static_cast<uint8_t>(op)));
		}
	};

	for (idx_t i = 0; i < cube_size; i++) {
		const double a_value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);

		if (DataCube::IsValidValue(a_value, a_no_data)) {
			*r_data_ptr = apply_op(a_value, b);
		} else {
			*r_data_ptr = a_value;
		}
		r_data_ptr++;
	}
}

DataCube DataCube::operator+(const DataCube &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::ADD, *this, other, result);
	return result;
}

DataCube DataCube::operator+(const double &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::ADD, *this, other, result);
	return result;
}

DataCube DataCube::operator-(const DataCube &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::SUBTRACT, *this, other, result);
	return result;
}

DataCube DataCube::operator-(const double &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::SUBTRACT, *this, other, result);
	return result;
}

DataCube DataCube::operator*(const DataCube &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::MULTIPLY, *this, other, result);
	return result;
}

DataCube DataCube::operator*(const double &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::MULTIPLY, *this, other, result);
	return result;
}

DataCube DataCube::operator/(const DataCube &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::DIVIDE, *this, other, result);
	return result;
}

DataCube DataCube::operator/(const double &other) const {
	DataCube result(allocator);
	Apply(CubeBinaryOp::DIVIDE, *this, other, result);
	return result;
}

} // namespace duckdb
