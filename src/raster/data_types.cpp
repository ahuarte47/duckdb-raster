#include "data_types.hpp"

#include <cmath>
#include <stdexcept>

// DuckDB
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//======================================================================================================================
// DataType & DataFormat
//======================================================================================================================

int64_t DataType::GetSizeBytes(const DataType::Value &data_type) {
	switch (data_type) {
	case DataType::Value::UINT8:
	case DataType::Value::INT8:
		return sizeof(uint8_t);
	case DataType::Value::UINT16:
	case DataType::Value::INT16:
		return sizeof(uint16_t);
	case DataType::Value::UINT32:
	case DataType::Value::INT32:
		return sizeof(uint32_t);
	case DataType::Value::UINT64:
	case DataType::Value::INT64:
		return sizeof(uint64_t);
	case DataType::Value::FLOAT:
		return sizeof(float);
	case DataType::Value::DOUBLE:
		return sizeof(double);
	default:
		throw std::invalid_argument("Invalid DataType value: " + std::to_string(static_cast<int>(data_type)));
	}
}

std::string DataType::ToString(const DataType::Value &data_type) {
	switch (data_type) {
	case DataType::Value::UINT8:
		return "uint8";
	case DataType::Value::INT8:
		return "int8";
	case DataType::Value::UINT16:
		return "uint16";
	case DataType::Value::INT16:
		return "int16";
	case DataType::Value::UINT32:
		return "uint32";
	case DataType::Value::INT32:
		return "int32";
	case DataType::Value::UINT64:
		return "uint64";
	case DataType::Value::INT64:
		return "int64";
	case DataType::Value::FLOAT:
		return "float";
	case DataType::Value::DOUBLE:
		return "double";
	default:
		throw std::invalid_argument("Invalid DataType value: " + std::to_string(static_cast<int>(data_type)));
	}
}

DataFormat::Value DataFormat::FromString(const std::string &format_str) {
	std::string fmt = StringUtil::Upper(format_str);
	if (fmt == "RAW") {
		return DataFormat::Value::RAW;
	}
	throw std::invalid_argument("Invalid DataFormat string: " + format_str);
}

std::string DataFormat::ToString(const DataFormat::Value &data_format) {
	switch (data_format) {
	case DataFormat::Value::RAW:
		return "RAW";
	default:
		throw std::invalid_argument("Invalid DataFormat value: " + std::to_string(static_cast<int>(data_format)));
	}
}

//======================================================================================================================
// CubeCellValue
//======================================================================================================================

bool CubeCellValue::IsNoDataValue(double value, double no_data) {
	return std::isnan(value) ? std::isnan(no_data) : value == no_data;
}

bool CubeCellValue::IsNoDataValue() const {
	return std::isnan(value) ? std::isnan(no_data) : value == no_data;
}

bool CubeCellValue::IsValidValue(double value, double no_data) {
	return !std::isnan(value) && !std::isinf(value) && value != no_data;
}

bool CubeCellValue::IsValidValue() const {
	return !std::isnan(value) && !std::isinf(value) && value != no_data;
}

bool CubeUnaryOp::Eval(CubeUnaryOp::Value op, const CubeCellValue &cell_value, double &result) {
	if (cell_value.IsValidValue()) {
		switch (op) {
		case CubeUnaryOp::Value::NEGATE:
			result = -cell_value.value;
			return true;
		case CubeUnaryOp::Value::ABSOLUTE:
			result = std::abs(cell_value.value);
			return true;
		case CubeUnaryOp::Value::SQUARE_ROOT:
			result = std::sqrt(cell_value.value);
			return true;
		case CubeUnaryOp::Value::LOGARITHM:
			result = std::log(cell_value.value);
			return true;
		case CubeUnaryOp::Value::EXPONENTIAL:
			result = std::exp(cell_value.value);
			return true;
		default:
			throw std::runtime_error("Unsupported operation: " + std::to_string(static_cast<uint8_t>(op)));
		}
	}
	return false;
}

bool CubeBinaryOp::Eval(CubeBinaryOp::Value op, const CubeCellValue &a, const CubeCellValue &b, double &result) {
	// OR is a special case: it selects the first non-nodata value between a and b, so it must
	// run even when one or both inputs are nodata — before the general validity guard below.
	if (op == CubeBinaryOp::Value::OR) {
		if (a.IsValidValue()) {
			result = a.value;
			return true;
		}
		if (b.IsValidValue()) {
			result = b.value;
			return true;
		}
		return false;
	}
	if (a.IsValidValue() && b.IsValidValue()) {
		switch (op) {
		case CubeBinaryOp::Value::EQUAL:
			result = (a.value == b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::NOT_EQUAL:
			result = (a.value != b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::GREATER:
			result = (a.value > b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::LESS:
			result = (a.value < b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::GREATER_EQUAL:
			result = (a.value >= b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::LESS_EQUAL:
			result = (a.value <= b.value) ? 1.0 : 0.0;
			return true;
		case CubeBinaryOp::Value::ADD:
			result = a.value + b.value;
			return true;
		case CubeBinaryOp::Value::SUBTRACT:
			result = a.value - b.value;
			return true;
		case CubeBinaryOp::Value::MULTIPLY:
			result = a.value * b.value;
			return true;
		case CubeBinaryOp::Value::DIVIDE:
			if (b.value != 0.0) {
				result = a.value / b.value;
			} else {
				result = a.value;
			}
			return true;
		case CubeBinaryOp::Value::POW:
			result = std::pow(a.value, b.value);
			return true;
		case CubeBinaryOp::Value::MOD:
			result = std::fmod(a.value, b.value);
			return true;
		case CubeBinaryOp::Value::SET:
			result = b.value;
			return true;
		case CubeBinaryOp::Value::MIN:
			result = (a.value < b.value) ? a.value : b.value;
			return true;
		case CubeBinaryOp::Value::MAX:
			result = (a.value > b.value) ? a.value : b.value;
			return true;
		default:
			throw std::runtime_error("Unsupported operation: " + std::to_string(static_cast<uint8_t>(op)));
		}
	}
	return false;
}

} // namespace duckdb
