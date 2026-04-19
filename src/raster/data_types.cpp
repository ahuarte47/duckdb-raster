#include "data_types.hpp"

#include <stdexcept>
#include "duckdb/common/string_util.hpp"

namespace duckdb {

int64_t DataType::GetSizeBytes(const Value &data_type) {
	switch (data_type) {
	case UINT8:
	case INT8:
		return sizeof(uint8_t);
	case UINT16:
	case INT16:
		return sizeof(uint16_t);
	case UINT32:
	case INT32:
		return sizeof(uint32_t);
	case UINT64:
	case INT64:
		return sizeof(uint64_t);
	case FLOAT:
		return sizeof(float);
	case DOUBLE:
		return sizeof(double);
	default:
		throw std::invalid_argument("Invalid DataType value: " + std::to_string(static_cast<int>(data_type)));
	}
}

std::string DataType::ToString(const DataType::Value &data_type) {
	switch (data_type) {
	case DataType::UINT8:
		return "uint8";
	case DataType::INT8:
		return "int8";
	case DataType::UINT16:
		return "uint16";
	case DataType::INT16:
		return "int16";
	case DataType::UINT32:
		return "uint32";
	case DataType::INT32:
		return "int32";
	case DataType::UINT64:
		return "uint64";
	case DataType::INT64:
		return "int64";
	case DataType::FLOAT:
		return "float";
	case DataType::DOUBLE:
		return "double";
	default:
		throw std::invalid_argument("Invalid DataType value: " + std::to_string(static_cast<int>(data_type)));
	}
}

DataFormat::Value DataFormat::FromString(const std::string &format_str) {
	std::string fmt = StringUtil::Upper(format_str);
	if (fmt == "RAW") {
		return DataFormat::RAW;
	}
	throw std::invalid_argument("Invalid DataFormat string: " + format_str);
}

std::string DataFormat::ToString(const DataFormat::Value &data_format) {
	switch (data_format) {
	case DataFormat::RAW:
		return "RAW";
	default:
		throw std::invalid_argument("Invalid DataFormat value: " + DataFormat::ToString(data_format));
	}
}

} // namespace duckdb
