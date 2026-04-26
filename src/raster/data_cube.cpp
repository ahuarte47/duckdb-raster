#include "data_cube.hpp"
#include "raster_types.hpp"

namespace duckdb {

//======================================================================================================================
// DataCube
//======================================================================================================================

DataCube::DataCube() : DataCube::DataCube(Allocator::DefaultAllocator()) {
}

DataCube::DataCube(Allocator &allocator)
    : allocator(allocator), header(), data_buffer(allocator), temp_buffer(allocator) {
}

DataCube DataCube::EMPTY_CUBE(DataType::Value data_type) {
	DataCube empty_cube;
	empty_cube.header.data_type = data_type;
	empty_cube.SetHeader(empty_cube.GetHeader(), false);
	return empty_cube;
}

DataHeader DataCube::GetHeader() const {
	return header;
}

void DataCube::SetHeader(const DataHeader &header, bool init_buffer) {
	this->header = header;

	// Reset the buffer and write the header to it.
	if (init_buffer) {
		data_buffer.Rewind();
		data_buffer.GrowCapacity(GetExpectedSizeBytes());
		data_buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));
	} else {
		data_buffer.Rewind();
		data_buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));
	}
}

const MemoryStream &DataCube::GetBuffer() const {
	return data_buffer;
}

MemoryStream &DataCube::GetBuffer() {
	return data_buffer;
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

	data_buffer.Rewind();
	data_buffer.GrowCapacity(blob_size);
	data_buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));

	const idx_t cube_size = GetCubeSize();
	if (cube_size > 0) {
		const idx_t data_size = blob_size - sizeof(DataHeader);
		data_buffer.WriteData(blob_data + sizeof(DataHeader), data_size);
	}
}

Value DataCube::ToBlob() const {
	Value blob = Value::BLOB(data_buffer.GetData(), data_buffer.GetPosition());
	blob.Reinterpret(RasterTypes::DATACUBE());
	return blob;
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
	header.data_format = DataFormat::Value::RAW;

	data_buffer.Rewind();
	data_buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));

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
		data_buffer.WriteData(const_data_ptr_cast(raw.data()), raw.size() * sizeof(T));
	};

	switch (header.data_type) {
	case DataType::Value::UINT8:
		write_values([](const Value &v) { return v.GetValue<uint8_t>(); });
		break;
	case DataType::Value::INT8:
		write_values([](const Value &v) { return v.GetValue<int8_t>(); });
		break;
	case DataType::Value::UINT16:
		write_values([](const Value &v) { return v.GetValue<uint16_t>(); });
		break;
	case DataType::Value::INT16:
		write_values([](const Value &v) { return v.GetValue<int16_t>(); });
		break;
	case DataType::Value::UINT32:
		write_values([](const Value &v) { return v.GetValue<uint32_t>(); });
		break;
	case DataType::Value::INT32:
		write_values([](const Value &v) { return v.GetValue<int32_t>(); });
		break;
	case DataType::Value::UINT64:
		write_values([](const Value &v) { return v.GetValue<uint64_t>(); });
		break;
	case DataType::Value::INT64:
		write_values([](const Value &v) { return v.GetValue<int64_t>(); });
		break;
	case DataType::Value::FLOAT:
		write_values([](const Value &v) { return v.GetValue<float>(); });
		break;
	case DataType::Value::DOUBLE:
		write_values([](const Value &v) { return v.GetValue<double>(); });
		break;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(header.data_type));
	}

	ChangeType(in_header.data_type);
}

Value DataCube::ToArray(const LogicalType &output_type, bool filter_nodata) {
	const DataType::Value data_type = header.data_type;
	const double no_data = header.no_data;
	vector<Value> data_values;

	idx_t cube_size = GetCubeSize();
	if (cube_size > 0) {
		data_values.reserve(cube_size);

		EnsureRaw();

		auto read_values = [&](auto read_raw, auto make_value) {
			if (filter_nodata) {
				for (idx_t i = 0; i < cube_size; i++) {
					const auto raw_value = read_raw();

					if (!CubeCellValue::IsNoDataValue(static_cast<double>(raw_value), no_data)) {
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

		const auto old_position = data_buffer.GetPosition();
		data_buffer.SetPosition(sizeof(DataHeader));

		switch (data_type) {
		case DataType::Value::UINT8:
			read_values([&] { return data_buffer.Read<uint8_t>(); }, [](auto v) { return Value::UTINYINT(v); });
			break;
		case DataType::Value::INT8:
			read_values([&] { return data_buffer.Read<int8_t>(); }, [](auto v) { return Value::TINYINT(v); });
			break;
		case DataType::Value::UINT16:
			read_values([&] { return data_buffer.Read<uint16_t>(); }, [](auto v) { return Value::USMALLINT(v); });
			break;
		case DataType::Value::INT16:
			read_values([&] { return data_buffer.Read<int16_t>(); }, [](auto v) { return Value::SMALLINT(v); });
			break;
		case DataType::Value::UINT32:
			read_values([&] { return data_buffer.Read<uint32_t>(); }, [](auto v) { return Value::UINTEGER(v); });
			break;
		case DataType::Value::INT32:
			read_values([&] { return data_buffer.Read<int32_t>(); }, [](auto v) { return Value::INTEGER(v); });
			break;
		case DataType::Value::UINT64:
			read_values([&] { return data_buffer.Read<uint64_t>(); }, [](auto v) { return Value::UBIGINT(v); });
			break;
		case DataType::Value::INT64:
			read_values([&] { return data_buffer.Read<int64_t>(); }, [](auto v) { return Value::BIGINT(v); });
			break;
		case DataType::Value::FLOAT:
			read_values([&] { return data_buffer.Read<float>(); }, [](auto v) { return Value::FLOAT(v); });
			break;
		case DataType::Value::DOUBLE:
			read_values([&] { return data_buffer.Read<double>(); }, [](auto v) { return Value::DOUBLE(v); });
			break;
		default: {
			data_buffer.SetPosition(old_position);
			throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
		}
		}

		data_buffer.SetPosition(old_position);
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
T DataCube::GetValue(uint32_t band, uint32_t col, uint32_t row) {
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
T DataCube::GetValue(idx_t index) {
	if (index >= GetCubeSize()) {
		throw std::out_of_range("Index is out of bounds");
	}

	EnsureRaw();

	const idx_t byte_offset = sizeof(DataHeader) + (index * DataType::GetSizeBytes(header.data_type));
	const auto value_ptr = data_buffer.GetData() + byte_offset;

	return *reinterpret_cast<const T *>(value_ptr);
}

bool DataCube::IsNullOrEmpty() {
	const idx_t cube_size = GetCubeSize();

	if (cube_size == 0) {
		return true;
	}

	EnsureRaw();

	const data_ptr_t data_ptr = data_buffer.GetData() + sizeof(DataHeader);

	for (idx_t i = 0; i < cube_size; i++) {
		const double value = ReadValueAs<double>(header.data_type, data_ptr, i);

		if (!CubeCellValue::IsNoDataValue(value, header.no_data)) {
			return false;
		}
	}
	return true;
}

template <typename T>
T DataCube::ReadValueAs(DataType::Value data_type, const data_ptr_t data_ptr, idx_t value_index) {
	switch (data_type) {
	case DataType::Value::UINT8:
		return static_cast<T>(*reinterpret_cast<const uint8_t *>(data_ptr + value_index * sizeof(uint8_t)));
	case DataType::Value::INT8:
		return static_cast<T>(*reinterpret_cast<const int8_t *>(data_ptr + value_index * sizeof(int8_t)));
	case DataType::Value::UINT16:
		return static_cast<T>(*reinterpret_cast<const uint16_t *>(data_ptr + value_index * sizeof(uint16_t)));
	case DataType::Value::INT16:
		return static_cast<T>(*reinterpret_cast<const int16_t *>(data_ptr + value_index * sizeof(int16_t)));
	case DataType::Value::UINT32:
		return static_cast<T>(*reinterpret_cast<const uint32_t *>(data_ptr + value_index * sizeof(uint32_t)));
	case DataType::Value::INT32:
		return static_cast<T>(*reinterpret_cast<const int32_t *>(data_ptr + value_index * sizeof(int32_t)));
	case DataType::Value::UINT64:
		return static_cast<T>(*reinterpret_cast<const uint64_t *>(data_ptr + value_index * sizeof(uint64_t)));
	case DataType::Value::INT64:
		return static_cast<T>(*reinterpret_cast<const int64_t *>(data_ptr + value_index * sizeof(int64_t)));
	case DataType::Value::FLOAT:
		return static_cast<T>(*reinterpret_cast<const float *>(data_ptr + value_index * sizeof(float)));
	case DataType::Value::DOUBLE:
		return static_cast<T>(*reinterpret_cast<const double *>(data_ptr + value_index * sizeof(double)));
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
	}
}

void DataCube::ChangeFormat(const DataFormat::Value &new_format) {
	if (header.data_format == new_format) {
		return;
	}
	throw std::runtime_error("Data format conversion is not implemented yet");
}

void DataCube::ChangeType(const DataType::Value &new_data_type) {
	if (header.data_type == new_data_type) {
		return;
	}

	const idx_t cube_size = GetCubeSize();

	DataHeader temp_header = header;
	temp_header.data_type = new_data_type;
	temp_header.data_format = DataFormat::Value::RAW;
	temp_buffer.Rewind();
	temp_buffer.GrowCapacity(cube_size * DataType::GetSizeBytes(new_data_type) + sizeof(DataHeader));
	temp_buffer.WriteData(const_data_ptr_cast(&temp_header), sizeof(DataHeader));

	const data_ptr_t data_ptr = data_buffer.GetData() + sizeof(DataHeader);
	data_ptr_t temp_ptr = temp_buffer.GetData() + sizeof(DataHeader);

	auto write_as = [&](auto *typed_ptr) {
		using T = std::remove_pointer_t<decltype(typed_ptr)>;
		constexpr double t_min = static_cast<double>(std::numeric_limits<T>::lowest());
		constexpr double t_max = static_cast<double>(std::numeric_limits<T>::max());
		const DataType::Value src_type = header.data_type;

		for (idx_t i = 0; i < cube_size; i++) {
			const double value = DataCube::ReadValueAs<double>(src_type, data_ptr, i);
			typed_ptr[i] = static_cast<T>(ClampValue<double>(value, t_min, t_max));
		}
	};

	switch (new_data_type) {
	case DataType::Value::UINT8:
		write_as(reinterpret_cast<uint8_t *>(temp_ptr));
		break;
	case DataType::Value::INT8:
		write_as(reinterpret_cast<int8_t *>(temp_ptr));
		break;
	case DataType::Value::UINT16:
		write_as(reinterpret_cast<uint16_t *>(temp_ptr));
		break;
	case DataType::Value::INT16:
		write_as(reinterpret_cast<int16_t *>(temp_ptr));
		break;
	case DataType::Value::UINT32:
		write_as(reinterpret_cast<uint32_t *>(temp_ptr));
		break;
	case DataType::Value::INT32:
		write_as(reinterpret_cast<int32_t *>(temp_ptr));
		break;
	case DataType::Value::UINT64:
		write_as(reinterpret_cast<uint64_t *>(temp_ptr));
		break;
	case DataType::Value::INT64:
		write_as(reinterpret_cast<int64_t *>(temp_ptr));
		break;
	case DataType::Value::FLOAT:
		write_as(reinterpret_cast<float *>(temp_ptr));
		break;
	case DataType::Value::DOUBLE:
		write_as(reinterpret_cast<double *>(temp_ptr));
		break;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(new_data_type));
	}

	header = temp_header;
	data_buffer.Rewind();
	data_buffer.WriteData(const_data_ptr_cast(&header), sizeof(DataHeader));
	data_buffer.WriteData(temp_ptr, cube_size * DataType::GetSizeBytes(new_data_type));
	temp_buffer.Rewind();
}

void DataCube::EnsureRaw() {
	ChangeFormat(DataFormat::Value::RAW);
}

void DataCube::Apply(const CubeUnaryCellFunc &func, DataCube &a, DataCube &r) {
	const idx_t cube_size = a.GetCubeSize();

	// If the cube is empty, just copy the header and return.
	if (cube_size == 0) {
		r.SetHeader(a.GetHeader(), false);
		return;
	}

	a.EnsureRaw();
	DataHeader r_header = a.header;
	r_header.data_type = DataType::Value::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.data_buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.data_buffer.GetData() + sizeof(DataHeader));

	const DataType::Value a_data_type = a.header.data_type;
	CubeCellValue a_cell_val = {0, 0.0, a.header.no_data};
	double result = 0.0;

	for (idx_t i = 0; i < cube_size; i++) {
		a_cell_val.index = i;
		a_cell_val.value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);

		// Write the result of the cell operation to the result cube.
		if (func(a_cell_val, result)) {
			*r_data_ptr = result;
		}
		r_data_ptr++;
	}
	r.data_buffer.SetPosition(sizeof(DataHeader) + cube_size * sizeof(double));
}

void DataCube::Apply(const CubeBinaryCellFunc &func, DataCube &a, DataCube &b, DataCube &r) {
	const idx_t cube_size = a.GetCubeSize();

	// If the cube is empty, just copy the header and return.
	if (cube_size == 0) {
		r.SetHeader(a.GetHeader(), false);
		return;
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

	a.EnsureRaw();
	b.EnsureRaw();
	DataHeader r_header = a.header;
	r_header.data_type = DataType::Value::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.data_buffer.GetData() + sizeof(DataHeader);
	data_ptr_t b_data_ptr = b.data_buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.data_buffer.GetData() + sizeof(DataHeader));

	const DataType::Value a_data_type = a.header.data_type;
	const DataType::Value b_data_type = b.header.data_type;
	CubeCellValue a_cell_val = {0, 0.0, a.header.no_data};
	CubeCellValue b_cell_val = {0, 0.0, b.header.no_data};
	double result = 0.0;

	for (idx_t i = 0; i < cube_size; i++) {
		a_cell_val.index = i;
		a_cell_val.value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);
		b_cell_val.index = i;
		b_cell_val.value = DataCube::ReadValueAs<double>(b_data_type, b_data_ptr, i);

		// Write the result of the cell operation to the result cube.
		if (func(a_cell_val, b_cell_val, result)) {
			*r_data_ptr = result;
		}
		r_data_ptr++;
	}
	r.data_buffer.SetPosition(sizeof(DataHeader) + cube_size * sizeof(double));
}

void DataCube::Apply(const CubeBinaryCellFunc &func, DataCube &a, const double &b, DataCube &r) {
	const idx_t cube_size = a.GetCubeSize();

	// If the cube is empty, just copy the header and return.
	if (cube_size == 0) {
		r.SetHeader(a.GetHeader(), false);
		return;
	}

	a.EnsureRaw();
	DataHeader r_header = a.header;
	r_header.data_type = DataType::Value::DOUBLE;
	r.SetHeader(r_header, true);

	data_ptr_t a_data_ptr = a.data_buffer.GetData() + sizeof(DataHeader);
	double *r_data_ptr = reinterpret_cast<double *>(r.data_buffer.GetData() + sizeof(DataHeader));

	const DataType::Value a_data_type = a.header.data_type;
	CubeCellValue a_cell_val = {0, 0.0, a.header.no_data};
	CubeCellValue b_cell_val = {0, b, NumericLimits<double>::Minimum()};
	double result = 0.0;

	for (idx_t i = 0; i < cube_size; i++) {
		a_cell_val.index = i;
		a_cell_val.value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);
		b_cell_val.index = i;

		// Write the result of the cell operation to the result cube.
		if (func(a_cell_val, b_cell_val, result)) {
			*r_data_ptr = result;
		}
		r_data_ptr++;
	}
	r.data_buffer.SetPosition(sizeof(DataHeader) + cube_size * sizeof(double));
}

void DataCube::Apply(const CubeCellFunc &func, DataCube &a) {
	const idx_t cube_size = a.GetCubeSize();

	// If the cube is empty, return doing nothing.
	if (cube_size == 0) {
		return;
	}

	a.EnsureRaw();

	data_ptr_t a_data_ptr = a.data_buffer.GetData() + sizeof(DataHeader);
	const DataType::Value a_data_type = a.header.data_type;
	CubeCellValue a_cell_val = {0, 0.0, a.header.no_data};

	for (idx_t i = 0; i < cube_size; i++) {
		a_cell_val.index = i;
		a_cell_val.value = DataCube::ReadValueAs<double>(a_data_type, a_data_ptr, i);

		// Call the cell function with the cell value.
		func(a_cell_val);
	}
}

DataCube DataCube::operator+(DataCube &other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::ADD, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator+(double other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::ADD, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator-(DataCube &other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::SUBTRACT, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator-(double other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::SUBTRACT, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator*(DataCube &other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::MULTIPLY, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator*(double other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::MULTIPLY, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator/(DataCube &other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::DIVIDE, a, b, out); },
	      *this, other, result);

	return result;
}

DataCube DataCube::operator/(double other) {
	DataCube result(allocator);

	Apply([](const CubeCellValue &a, const CubeCellValue &b,
	         double &out) { return CubeBinaryOp::Eval(CubeBinaryOp::DIVIDE, a, b, out); },
	      *this, other, result);

	return result;
}

} // namespace duckdb
