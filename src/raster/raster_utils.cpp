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
