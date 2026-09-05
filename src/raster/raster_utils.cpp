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
		return RasterCoord(static_cast<int32_t>(std::floor(col)), static_cast<int32_t>(std::floor(row)));
	}

	const double det = matrix[1] * matrix[5] - matrix[2] * matrix[4];
	if (det == 0) {
		throw InvalidInputException("Affine transform matrix is not invertible.");
	}
	const double inv_det = 1.0 / det;
	const double col = inv_det * (+matrix[5] * (x - matrix[0]) - matrix[2] * (y - matrix[3]));
	const double row = inv_det * (-matrix[4] * (x - matrix[0]) + matrix[1] * (y - matrix[3]));

	return RasterCoord(static_cast<int32_t>(std::floor(col)), static_cast<int32_t>(std::floor(row)));
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

std::string RasterUtils::GetMetadataOfDataset(GDALDataset *dataset) {
	if (!dataset) {
		throw InvalidInputException("GDAL dataset is null.");
	}

	const int raster_size_x = dataset->GetRasterXSize();
	const int raster_size_y = dataset->GetRasterYSize();

	int block_size_x, block_size_y;
	GDALRasterBand *band = dataset->GetRasterBand(1);
	band->GetBlockSize(&block_size_x, &block_size_y);

	std::string crs = "";
	const char *proj_ref = dataset->GetProjectionRef();

	if (proj_ref && strlen(proj_ref) > 0) {
		int srid = RasterUtils::GetSrid(proj_ref);
		if (srid != 0) {
			crs = StringUtil::Format("EPSG:%d", srid);
		} else {
			crs = proj_ref;
		}
	}

	double gt[6] = {0};
	if (dataset->GetGeoTransform(gt) != CE_None) {
		gt[1] = 1.0;
		gt[5] = -1.0;
	}

	const Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(gt, 0, 0);
	const Point2D pt1 = RasterUtils::RasterCoordToWorldCoord(gt, raster_size_x, 0);
	const Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(gt, raster_size_x, raster_size_y);
	const Point2D pt3 = RasterUtils::RasterCoordToWorldCoord(gt, 0, raster_size_y);

	const double x_min = MinValue<double>(MinValue<double>(pt0.x, pt1.x), MinValue<double>(pt2.x, pt3.x));
	const double y_min = MinValue<double>(MinValue<double>(pt0.y, pt1.y), MinValue<double>(pt2.y, pt3.y));
	const double x_max = MaxValue<double>(MaxValue<double>(pt0.x, pt1.x), MaxValue<double>(pt2.x, pt3.x));
	const double y_max = MaxValue<double>(MaxValue<double>(pt0.y, pt1.y), MaxValue<double>(pt2.y, pt3.y));

	const std::string geometry_wkt = StringUtil::Format("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))", pt0.x, pt0.y,
	                                                    pt1.x, pt1.y, pt2.x, pt2.y, pt3.x, pt3.y, pt0.x, pt0.y);

	std::ostringstream metadata_ds;
	metadata_ds << std::fixed;
	metadata_ds << "{";
	metadata_ds << "\"file_format\": \"raster\", ";
	metadata_ds << "\"version\": \"0.1.0\", ";
	metadata_ds << "\"data_format\": \"RAW\", ";
	metadata_ds << "\"datacube\": false, ";
	metadata_ds << "\"crs\": \"" << crs << "\", ";
	metadata_ds << "\"transform\": [" << gt[0] << ", " << gt[1] << ", " << gt[2] << ", " << gt[3] << ", " << gt[4]
	            << ", " << gt[5] << "], ";
	metadata_ds << "\"bounds\": [" << x_min << ", " << y_min << ", " << x_max << ", " << y_max << "], ";
	metadata_ds << "\"geometry\": \"" << geometry_wkt << "\", ";
	metadata_ds << "\"width\": " << raster_size_x << ", ";
	metadata_ds << "\"height\": " << raster_size_y << ", ";
	metadata_ds << "\"blocksize_x\": " << block_size_x << ", ";
	metadata_ds << "\"blocksize_y\": " << block_size_y << ", ";
	metadata_ds << "\"band_count\": " << dataset->GetRasterCount() << ", ";
	metadata_ds << "\"bands\": [";

	for (int b = 1; b <= dataset->GetRasterCount(); b++) {
		GDALRasterBand *band = dataset->GetRasterBand(b);

		int has_nodata = 0;
		int has_scale = 0;
		int has_offset = 0;
		const GDALDataType data_type = band->GetRasterDataType();
		const char *label = band->GetDescription();
		const double nodata = band->GetNoDataValue(&has_nodata);
		const double scale = band->GetScale(&has_scale);
		const double offset = band->GetOffset(&has_offset);
		const GDALColorInterp color_interp = band->GetColorInterpretation();
		const char *unit_type = band->GetUnitType();

		std::string band_name = StringUtil::Format("band_%d", b);
		metadata_ds << "{";
		metadata_ds << "\"name\": \"band_" << (b - 1) << "\", ";
		metadata_ds << "\"description\": \"" << (label && strlen(label) > 0 ? label : "") << "\", ";
		metadata_ds << "\"type_name\": \"" << GDALGetDataTypeName(data_type) << "\", ";
		metadata_ds << "\"data_type\": " << data_type << ", ";
		metadata_ds << "\"data_size\": " << GDALGetDataTypeSizeBytes(data_type) << ", ";
		metadata_ds << "\"width\": " << block_size_x << ", ";
		metadata_ds << "\"height\": " << block_size_y << ", ";
		metadata_ds << "\"colorinterp\": " << color_interp << ", ";
		metadata_ds << "\"nodata\": " << (has_nodata ? std::to_string(nodata) : "null") << ", ";
		metadata_ds << "\"scale\": " << (has_scale ? std::to_string(scale) : "null") << ", ";
		metadata_ds << "\"offset\": " << (has_offset ? std::to_string(offset) : "null") << ", ";
		metadata_ds << "\"unit\": \"" << (unit_type && strlen(unit_type) > 0 ? unit_type : "") << "\"";
		metadata_ds << "}";

		if (b < dataset->GetRasterCount()) {
			metadata_ds << ", ";
		}
	}
	metadata_ds << "]";
	metadata_ds << "}";

	return metadata_ds.str();
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

LogicalType RasterUtils::GdalTypeToLogicalType(const GDALDataType &data_type) {
	switch (data_type) {
	case GDT_Byte:
		return LogicalType::UTINYINT;
	case GDT_Int8:
		return LogicalType::TINYINT;
	case GDT_UInt16:
		return LogicalType::USMALLINT;
	case GDT_Int16:
		return LogicalType::SMALLINT;
	case GDT_UInt32:
		return LogicalType::UINTEGER;
	case GDT_Int32:
		return LogicalType::INTEGER;
	case GDT_UInt64:
		return LogicalType::UBIGINT;
	case GDT_Int64:
		return LogicalType::BIGINT;
	case GDT_Float32:
		return LogicalType::FLOAT;
	case GDT_Float64:
		return LogicalType::DOUBLE;
	// Note: GDAL's Float16 is not directly supported in DuckDB, we could map it to DOUBLE.
	case GDT_Float16:
	default:
		throw std::runtime_error("Unsupported GDALDataType: " + std::string(GDALGetDataTypeName(data_type)));
	}
};

} // namespace duckdb
