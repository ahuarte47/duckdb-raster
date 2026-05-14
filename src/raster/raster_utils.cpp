#include "raster_utils.hpp"

#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/vector.hpp"
#include "yyjson.hpp"
using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//======================================================================================================================
// Raster utilities
//======================================================================================================================

std::string RasterUtils::GetLastGdalErrorMsg() {
	return std::string(CPLGetLastErrorMsg());
}

Point2D RasterUtils::RasterCoordToWorldCoord(const double (&matrix)[6], const int32_t &col, const int32_t &row) {
	const double x = matrix[0] + matrix[1] * col + matrix[2] * row;
	const double y = matrix[3] + matrix[4] * col + matrix[5] * row;
	return Point2D(x, y);
}

Point2D RasterUtils::RasterCoordToWorldCoord(const double (&matrix)[6], const RasterCoord &coord) {
	const double x = matrix[0] + matrix[1] * coord.col + matrix[2] * coord.row;
	const double y = matrix[3] + matrix[4] * coord.col + matrix[5] * coord.row;
	return Point2D(x, y);
}

RasterCoord RasterUtils::WorldCoordToRasterCoord(const double (&matrix)[6], const double &x, const double &y) {
	// Special case: no rotation/skew, to avoid computing the determinant.
	if (matrix[2] == 0.0 && matrix[4] == 0.0 && matrix[1] != 0.0 && matrix[5] != 0.0) {
		const double col = (x - matrix[0]) / matrix[1];
		const double row = (y - matrix[3]) / matrix[5];
		return RasterCoord(static_cast<int32_t>(std::round(col)), static_cast<int32_t>(std::round(row)));
	}

	const double det = matrix[1] * matrix[5] - matrix[2] * matrix[4];
	if (det == 0) {
		throw InvalidInputException("Affine transform matrix is not invertible.");
	}
	const double inv_det = 1.0 / det;
	const double col = inv_det * (+matrix[5] * (x - matrix[0]) - matrix[2] * (y - matrix[3]));
	const double row = inv_det * (-matrix[4] * (x - matrix[0]) + matrix[1] * (y - matrix[3]));

	return RasterCoord(static_cast<int32_t>(std::round(col)), static_cast<int32_t>(std::round(row)));
}

RasterCoord RasterUtils::WorldCoordToRasterCoord(const double (&matrix)[6], const Point2D &coord) {
	return WorldCoordToRasterCoord(matrix, coord.x, coord.y);
}

int RasterUtils::GetSrid(const char *proj_def) {
	int srid = 0; // SRID_UNKNOWN

	if (proj_def && strlen(proj_def) > 0) {
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

RasterTransformMatrix RasterUtils::GetTransformMatrix(const string &metadata) {
	RasterTransformMatrix matrix;

	const auto json_data = yyjson_read(metadata.c_str(), metadata.size(), YYJSON_READ_NOFLAG);
	if (!json_data) {
		throw IOException("Failed to parse the raster transform from tile metadata.");
	}

	// Parse metadata and populate 'affine' and 'blocksize_x/y' items.
	try {
		yyjson_val *root_val = yyjson_doc_get_root(json_data);
		yyjson_val *item_val = nullptr;

		if (!yyjson_is_obj(root_val)) {
			throw IOException("Invalid tile metadata format.");
		}

		// Affine transform is expected to be an array of 6 numeric values.

		if (!yyjson_is_arr(item_val = yyjson_obj_get(root_val, "transform"))) {
			throw InvalidInputException("Missing or incorrect 'transform' attribute in tile metadata.");
		} else if (yyjson_arr_size(item_val) != 6) {
			throw InvalidInputException("The 'transform' array in tile metadata must have exactly 6 elements.");
		} else {
			for (size_t i = 0; i < 6; i++) {
				auto val = yyjson_arr_get(item_val, i);
				if (!yyjson_is_num(val)) {
					throw InvalidInputException("Non-numeric value found in 'transform' array of tile metadata.");
				}
				matrix.affine[i] = yyjson_get_num(val);
			}
		}

		// Block size is expected to be numeric values.

		if (!yyjson_is_num(item_val = yyjson_obj_get(root_val, "blocksize_x"))) {
			throw InvalidInputException("Missing or incorrect 'blocksize_x' attribute in tile metadata.");
		} else {
			auto val = unsafe_yyjson_get_uint(item_val);
			matrix.blocksize_x = static_cast<int32_t>(val);
		}

		if (!yyjson_is_num(item_val = yyjson_obj_get(root_val, "blocksize_y"))) {
			throw InvalidInputException("Missing or incorrect 'blocksize_y' attribute in tile metadata.");
		} else {
			auto val = unsafe_yyjson_get_uint(item_val);
			matrix.blocksize_y = static_cast<int32_t>(val);
		}

		// Make sure to free the JSON document
		yyjson_doc_free(json_data);
	} catch (...) {
		// Make sure to free the JSON document in case of an exception
		yyjson_doc_free(json_data);
		throw;
	}
	return matrix;
}

DataType::Value RasterUtils::LogicalTypeToDataType(const LogicalType &data_type) {
	switch (data_type.id()) {
	case LogicalTypeId::UTINYINT:
		return DataType::Value::UINT8;
	case LogicalTypeId::TINYINT:
		return DataType::Value::INT8;
	case LogicalTypeId::USMALLINT:
		return DataType::Value::UINT16;
	case LogicalTypeId::SMALLINT:
		return DataType::Value::INT16;
	case LogicalTypeId::UINTEGER:
		return DataType::Value::UINT32;
	case LogicalTypeId::INTEGER:
		return DataType::Value::INT32;
	case LogicalTypeId::UBIGINT:
		return DataType::Value::UINT64;
	case LogicalTypeId::BIGINT:
		return DataType::Value::INT64;
	case LogicalTypeId::FLOAT:
		return DataType::Value::FLOAT;
	case LogicalTypeId::DOUBLE:
		return DataType::Value::DOUBLE;
	default:
		throw std::runtime_error("Unsupported LogicalType: " + data_type.ToString());
	}
}

LogicalType RasterUtils::DataTypeToLogicalType(const DataType::Value &data_type) {
	switch (data_type) {
	case DataType::Value::UINT8:
		return LogicalType::UTINYINT;
	case DataType::Value::INT8:
		return LogicalType::TINYINT;
	case DataType::Value::UINT16:
		return LogicalType::USMALLINT;
	case DataType::Value::INT16:
		return LogicalType::SMALLINT;
	case DataType::Value::UINT32:
		return LogicalType::UINTEGER;
	case DataType::Value::INT32:
		return LogicalType::INTEGER;
	case DataType::Value::UINT64:
		return LogicalType::UBIGINT;
	case DataType::Value::INT64:
		return LogicalType::BIGINT;
	case DataType::Value::FLOAT:
		return LogicalType::FLOAT;
	case DataType::Value::DOUBLE:
		return LogicalType::DOUBLE;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
	}
}

DataType::Value RasterUtils::GdalTypeToDataType(const GDALDataType &data_type) {
	switch (data_type) {
	case GDT_Byte:
		return DataType::Value::UINT8;
	case GDT_Int8:
		return DataType::Value::INT8;
	case GDT_UInt16:
		return DataType::Value::UINT16;
	case GDT_Int16:
		return DataType::Value::INT16;
	case GDT_UInt32:
		return DataType::Value::UINT32;
	case GDT_Int32:
		return DataType::Value::INT32;
	case GDT_UInt64:
		return DataType::Value::UINT64;
	case GDT_Int64:
		return DataType::Value::INT64;
	case GDT_Float32:
		return DataType::Value::FLOAT;
	case GDT_Float64:
		return DataType::Value::DOUBLE;
	// Note: GDAL's Float16 is not directly supported in DuckDB, we could map it to DOUBLE.
	case GDT_Float16:
	default:
		throw std::runtime_error("Unsupported GDALDataType: " + std::string(GDALGetDataTypeName(data_type)));
	}
}

GDALDataType RasterUtils::DataTypeToGdalType(const DataType::Value &data_type) {
	switch (data_type) {
	case DataType::Value::UINT8:
		return GDT_Byte;
	case DataType::Value::INT8:
		return GDT_Int8;
	case DataType::Value::UINT16:
		return GDT_UInt16;
	case DataType::Value::INT16:
		return GDT_Int16;
	case DataType::Value::UINT32:
		return GDT_UInt32;
	case DataType::Value::INT32:
		return GDT_Int32;
	case DataType::Value::UINT64:
		return GDT_UInt64;
	case DataType::Value::INT64:
		return GDT_Int64;
	case DataType::Value::FLOAT:
		return GDT_Float32;
	case DataType::Value::DOUBLE:
		return GDT_Float64;
	default:
		throw std::runtime_error("Unsupported DataType: " + DataType::ToString(data_type));
	}
}

} // namespace duckdb
