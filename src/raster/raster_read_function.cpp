#include "raster_read_function.hpp"
#include "raster_types.hpp"
#include "raster_utils.hpp"
#include "function_builder.hpp"
#include <sstream>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

// GDAL
#include "gdal_priv.h"

// Debug logging controlled by RASTER_DEBUG environment variable
static int GetDebugLevel() {
	static int level = -1;
	if (level == -1) {
		const char *env = std::getenv("RASTER_DEBUG");
		level = env ? std::atoi(env) : 0;
	}
	return level;
}

#define RASTER_SCAN_DEBUG_LOG(level, fmt, ...)                                                                         \
	do {                                                                                                               \
		if (GetDebugLevel() >= level) {                                                                                \
			fprintf(stderr, "RASTER: " fmt "\n", ##__VA_ARGS__);                                                       \
		}                                                                                                              \
	} while (0)

namespace duckdb {

namespace {

//======================================================================================================================
// Utilities
//======================================================================================================================

//! Convert a named parameter map to an array suitable for passing to GDAL functions.
static std::vector<char const *> NamedParametersAsVector(const named_parameter_map_t &input,
                                                         const std::string &keyname) {
	auto output = std::vector<char const *>();

	auto input_param = input.find(keyname);
	if (input_param != input.end()) {
		output.reserve(input.size() + 1);

		for (auto &param : ListValue::GetChildren(input_param->second)) {
			output.push_back(StringValue::Get(param).c_str());
		}
		output.push_back(nullptr);
	}
	return output;
}

//======================================================================================================================
// RT_Read
//======================================================================================================================

struct RT_Read {
	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		string file_name;
		named_parameter_map_t parameters;
		CompressionAlg::Value compression_alg = CompressionAlg::NONE;
		bool datacube = false;

		GDALDatasetUniquePtr dataset;
		std::string metadata_ds;
		std::string crs;
		double geo_transform[6] = {0};
		GDALDataType data_type = GDT_Unknown;
		double nodata_value = NumericLimits<double>::Minimum();
		int block_size_x = 0;
		int block_size_y = 0;
		int tiles_x = 0;
		int tiles_y = 0;

		vector<LogicalType> column_types;
		idx_t row_offset = 0;
		idx_t row_count = 0;

		~BindData() override {
			// Ensure the GDAL dataset is properly closed when the bind data is destroyed.
			dataset.reset();

			RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset closed: '%s'", file_name.c_str());
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names) {
		auto file_path = input.inputs[0].GetValue<string>();
		auto params = input.named_parameters;

		CompressionAlg::Value compression_alg = CompressionAlg::NONE;
		if (params.find("compression") != params.end()) {
			compression_alg = CompressionAlg::FromString(params["compression"].GetValue<string>());
		}

		bool datacube = false;
		if (params.find("datacube") != params.end()) {
			datacube = params["datacube"].GetValue<bool>();
		}

		// Open the dataset.

		const auto gdal_options = NamedParametersAsVector(params, "open_options");
		const auto gdal_drivers = NamedParametersAsVector(params, "allowed_drivers");
		const auto gdal_sibling = NamedParametersAsVector(params, "sibling_files");

		GDALDatasetUniquePtr dataset;
		dataset = GDALDatasetUniquePtr(GDALDataset::Open(file_path.c_str(), GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR,
		                                                 gdal_drivers.empty() ? nullptr : gdal_drivers.data(),
		                                                 gdal_options.empty() ? nullptr : gdal_options.data(),
		                                                 gdal_sibling.empty() ? nullptr : gdal_sibling.data()));

		if (!dataset.get()) {
			const std::string error = RasterUtils::GetLastGdalErrorMsg();
			throw IOException("Could not open file: " + file_path + " (" + error + ")");
		}

		RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset opened: '%s'", file_path.c_str());

		// Fetch the dataset metadata.

		double gt[6] = {0};

		if (dataset->GetGeoTransform(gt) != CE_None) {
			gt[1] = 1.0;
			gt[5] = -1.0;
		}

		RASTER_SCAN_DEBUG_LOG(1, " > GeoTransform: [%f, %f, %f, %f, %f, %f]", gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]);

		std::string crs = "";
		const char *proj_ref = dataset->GetProjectionRef();
		std::vector<std::string> band_names;

		if (proj_ref && strlen(proj_ref) > 0) {
			int srid = RasterUtils::GetSrid(proj_ref);
			if (srid != 0) {
				crs = StringUtil::Format("EPSG:%d", srid);
			} else {
				crs = proj_ref;
			}
		}

		RASTER_SCAN_DEBUG_LOG(1, " > CRS: '%s'", crs.c_str());

		// Calculate the total number of rows in the dataset (i.e. number of tiles).

		const int raster_size_x = dataset->GetRasterXSize();
		const int raster_size_y = dataset->GetRasterYSize();

		int block_size_x, block_size_y;
		GDALRasterBand *band = dataset->GetRasterBand(1);
		band->GetBlockSize(&block_size_x, &block_size_y);

		if (params.find("blocksize_x") != params.end()) {
			block_size_x = params["blocksize_x"].GetValue<int>();
		}
		if (params.find("blocksize_y") != params.end()) {
			block_size_y = params["blocksize_y"].GetValue<int>();
		}
		if (block_size_x <= 0 || block_size_y <= 0) {
			throw InvalidInputException("Invalid block size specified, must be greater than 0");
		}

		const int tiles_x = static_cast<int>((raster_size_x + block_size_x - 1) / block_size_x);
		const int tiles_y = static_cast<int>((raster_size_y + block_size_y - 1) / block_size_y);
		const int row_count = tiles_x * tiles_y;

		RASTER_SCAN_DEBUG_LOG(1, " > Raster size: %d x %d", raster_size_x, raster_size_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Block size: %d x %d", block_size_x, block_size_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Tiles: %d x %d", tiles_x, tiles_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Total rows (tiles): %d", row_count);
		RASTER_SCAN_DEBUG_LOG(1, " > Compression: '%s'", CompressionAlg::ToString(compression_alg).c_str());

		// Fill the column definitions.

		names.emplace_back("id");
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("x");
		return_types.emplace_back(LogicalType::DOUBLE);
		names.emplace_back("y");
		return_types.emplace_back(LogicalType::DOUBLE);
		names.emplace_back("bbox");
		return_types.emplace_back(RasterTypes::BBOX());
		names.emplace_back("geometry");
		return_types.emplace_back(crs.empty() ? LogicalType::GEOMETRY() : LogicalType::GEOMETRY(crs));
		names.emplace_back("level");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("tile_x");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("tile_y");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("cols");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("rows");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("metadata");
		return_types.emplace_back(LogicalType::JSON());

		GDALDataType data_type = GDT_Unknown;
		double nodata_value = NumericLimits<double>::Minimum();

		vector<LogicalType> column_types;
		for (int i = 0; i < RASTER_FIRST_BAND_COLUMN_INDEX; i++) {
			column_types.emplace_back(return_types[i]);
		}

		for (int b = 1; b <= dataset->GetRasterCount(); b++) {
			GDALRasterBand *band = dataset->GetRasterBand(b);

			std::string band_name = StringUtil::Format("databand_%d", b);
			band_names.emplace_back(band_name);

			const GDALDataType raster_type = band->GetRasterDataType();
			const char *type_name = GDALGetDataTypeName(raster_type);
			int has_nodata = 0;
			double nodata = band->GetNoDataValue(&has_nodata);
			nodata = has_nodata ? nodata : NumericLimits<double>::Minimum();

			RASTER_SCAN_DEBUG_LOG(1, " > Band %d: name='%s', type=%s, nodata=%f", b, band_name.c_str(), type_name,
			                      nodata);

			// Set the data type based on the first band, all subsequent bands must have the same type.
			if (b == 1) {
				data_type = raster_type;
				nodata_value = nodata;
			} else if (raster_type != data_type) {
				throw IOException("All bands must have the same data type");
			} else if (nodata != nodata_value) {
				throw IOException("All bands must have the same nodata value");
			}

			// For datacubes, we return a single BLOB column containing all the bands.
			if (!datacube || b == 1) {
				band_name = datacube ? "datacube" : StringUtil::Format("databand_%d", b);
				names.emplace_back(band_name);
				return_types.emplace_back(LogicalType::BLOB);
			}
		}

		// Get metadata from the dataset.

		const Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(gt, 0, 0);
		const Point2D pt1 = RasterUtils::RasterCoordToWorldCoord(gt, raster_size_x, 0);
		const Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(gt, raster_size_x, raster_size_y);
		const Point2D pt3 = RasterUtils::RasterCoordToWorldCoord(gt, 0, raster_size_y);

		const double x_min = MinValue<double>(MinValue<double>(pt0.x, pt1.x), MinValue<double>(pt2.x, pt3.x));
		const double y_min = MinValue<double>(MinValue<double>(pt0.y, pt1.y), MinValue<double>(pt2.y, pt3.y));
		const double x_max = MaxValue<double>(MaxValue<double>(pt0.x, pt1.x), MaxValue<double>(pt2.x, pt3.x));
		const double y_max = MaxValue<double>(MaxValue<double>(pt0.y, pt1.y), MaxValue<double>(pt2.y, pt3.y));

		const std::string geometry_wkt =
		    StringUtil::Format("POLYGON((%f %f, %f %f, %f %f, %f %f, %f %f))", pt0.x, pt0.y, pt1.x, pt1.y, pt2.x, pt2.y,
		                       pt3.x, pt3.y, pt0.x, pt0.y);

		std::ostringstream metadata_ds;
		metadata_ds << std::fixed;
		metadata_ds << "{";
		metadata_ds << "\"file_format\": \"raster\", ";
		metadata_ds << "\"version\": \"0.1.0\", ";
		metadata_ds << "\"compression\": \"" << CompressionAlg::ToString(compression_alg) << "\", ";
		metadata_ds << "\"datacube\": " << (datacube ? "true" : "false") << ", ";
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

			metadata_ds << "{";
			metadata_ds << "\"name\": \"" << band_names[b - 1] << "\", ";
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

		// Return the bind data.

		auto result = make_uniq<BindData>();
		result->file_name = file_path;
		result->parameters = params;
		result->compression_alg = compression_alg;
		result->datacube = datacube;
		result->dataset = std::move(dataset);
		result->metadata_ds = metadata_ds.str();
		result->crs = std::move(crs);
		std::copy(std::begin(gt), std::end(gt), std::begin(result->geo_transform));
		result->data_type = data_type;
		result->nodata_value = nodata_value;
		result->block_size_x = block_size_x;
		result->block_size_y = block_size_y;
		result->tiles_x = tiles_x;
		result->tiles_y = tiles_y;
		result->column_types = std::move(column_types);
		result->row_offset = 0;
		result->row_count = row_count;

		return std::move(result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init Global
	//------------------------------------------------------------------------------------------------------------------

	struct State final : GlobalTableFunctionState {
		idx_t current_rid;
		idx_t current_row;
		explicit State() : current_rid(0), current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq_base<GlobalTableFunctionState, State>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init Local
	//------------------------------------------------------------------------------------------------------------------

	//------------------------------------------------------------------------------------------------------------------
	// Optimize (Only LIMIT pushdown is implemented)
	//------------------------------------------------------------------------------------------------------------------

	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op) {
		// Apply optimizations on the LogicalPlan

		if (op->type == LogicalOperatorType::LOGICAL_LIMIT) {
			auto &limit = op->Cast<LogicalLimit>();

			// Only push down simple LIMIT & OFFSET without ORDER BY or GROUP BY, and with constant values,
			// as it would change the result of the query.
			if (limit.limit_val.Type() != LimitNodeType::CONSTANT_VALUE) {
				return;
			}
			if (limit.offset_val.Type() != LimitNodeType::UNSET &&
			    limit.offset_val.Type() != LimitNodeType::CONSTANT_VALUE) {
				return;
			}
			for (const auto &child : op->children) {
				if (child->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
					return;
				}
				if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
					return;
				}
				if (child->type == LogicalOperatorType::LOGICAL_GET) {
					auto &get = child->Cast<LogicalGet>();

					if (StringUtil::Lower(get.function.name) == "rt_read") {
						auto &bind_data = get.bind_data->Cast<BindData>();

						if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							const idx_t offset_value = limit.offset_val.GetConstantValue();
							RASTER_SCAN_DEBUG_LOG(1, "OFFSET pushdown: %llu", offset_value);
							bind_data.row_offset = offset_value;
							limit.offset_val = BoundLimitNode();
						}
						const idx_t limit_value = limit.limit_val.GetConstantValue();
						RASTER_SCAN_DEBUG_LOG(1, "LIMIT pushdown: %llu", limit_value);
						bind_data.row_count = MinValue<idx_t>(bind_data.row_count, bind_data.row_offset + limit_value);
						limit.limit_val = BoundLimitNode();
						return;
					}
				}
			}
		}

		// Recurse into children
		for (auto &child : op->children) {
			Optimize(input, child);
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Complex Filter Pushdown
	//------------------------------------------------------------------------------------------------------------------

	static void PushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
	                                  vector<unique_ptr<Expression>> &expressions) {
		auto &bind_data = bind_data_p->Cast<BindData>();

		// Get column_ids from LogicalGet to map expression column indices to table columns.

		const auto &get_column_ids = get.GetColumnIds();

		std::vector<column_t> column_ids;
		for (const auto &col_idx : get_column_ids) {
			column_ids.push_back(col_idx.IsVirtualColumn() ? COLUMN_IDENTIFIER_ROW_ID : col_idx.GetPrimaryIndex());
		}
		bind_data.column_ids = std::move(column_ids);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Cardinality
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<NodeStatistics> Cardinality(ClientContext &context, const FunctionData *data) {
		auto &bind_data = data->Cast<BindData>();
		auto result = make_uniq<NodeStatistics>();

		// This is the maximum number of rows/tiles in a single file
		result->has_max_cardinality = true;
		result->max_cardinality = bind_data.row_count - bind_data.row_offset;

		return result;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Fill the output chunk with data for the specified tile coordinates.
	static bool FillOutput(const BindData &bind_data, const idx_t &row_id, const idx_t &row_index, const int &level,
	                       const int &tile_x, const int &tile_y, MemoryStream &data_buffer, DataChunk &output) {
		GDALDataset *dataset = bind_data.dataset.get();

		const auto &geo_transform = bind_data.geo_transform;
		const int &raster_size_x = dataset->GetRasterXSize();
		const int &raster_size_y = dataset->GetRasterYSize();
		const int &block_size_x = bind_data.block_size_x;
		const int &block_size_y = bind_data.block_size_y;
		const auto &column_ids = bind_data.column_ids;

		const int offset_x = tile_x * block_size_x;
		const int offset_y = tile_y * block_size_y;
		const int size_x = MinValue<int>(block_size_x, raster_size_x - offset_x);
		const int size_y = MinValue<int>(block_size_y, raster_size_y - offset_y);

		const Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(geo_transform, offset_x, offset_y);
		const Point2D pt1 = RasterUtils::RasterCoordToWorldCoord(geo_transform, offset_x + size_x, offset_y);
		const Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(geo_transform, offset_x + size_x, offset_y + size_y);
		const Point2D pt3 = RasterUtils::RasterCoordToWorldCoord(geo_transform, offset_x, offset_y + size_y);

		const double x_min = MinValue<double>(MinValue<double>(pt0.x, pt1.x), MinValue<double>(pt2.x, pt3.x));
		const double y_min = MinValue<double>(MinValue<double>(pt0.y, pt1.y), MinValue<double>(pt2.y, pt3.y));
		const double x_max = MaxValue<double>(MaxValue<double>(pt0.x, pt1.x), MaxValue<double>(pt2.x, pt3.x));
		const double y_max = MaxValue<double>(MaxValue<double>(pt0.y, pt1.y), MaxValue<double>(pt2.y, pt3.y));

		// RASTER_SCAN_DEBUG_LOG(1, " > txy=(%d, %d): t_coords=(%d, %d, %d, %d), bbox=(%f, %f, %f, %f)", tile_x, tile_y,
		//                       offset_x, ofnfset_y, size_x, size_y, x_min, y_min, x_max, y_max);

		// Fill the output chunk for the current row.
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			const auto &dim_index = column_ids[col_idx];

			switch (dim_index) {
			case RASTER_ROWID_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::BIGINT(row_id));
				break;
			case RASTER_X_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::DOUBLE(pt0.x + 0.5 * (pt2.x - pt0.x)));
				break;
			case RASTER_Y_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::DOUBLE(pt0.y + 0.5 * (pt2.y - pt0.y)));
				break;
			case RASTER_BBOX_COLUMN_INDEX: {
				const Value bbox = Value::STRUCT({{"xmin", x_min}, {"ymin", y_min}, {"xmax", x_max}, {"ymax", y_max}});
				output.data[col_idx].SetValue(row_index, bbox);
				break;
			}
			case RASTER_GEOMETRY_COLUMN_INDEX: {
				const std::string geometry_wkt =
				    StringUtil::Format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", pt0.x, pt0.y, pt1.x, pt1.y,
				                       pt2.x, pt2.y, pt3.x, pt3.y, pt0.x, pt0.y);

				output.data[col_idx].SetValue(row_index, geometry_wkt);
				break;
			}
			case RASTER_LEVEL_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::INTEGER(level));
				break;
			case RASTER_COL_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::INTEGER(tile_x));
				break;
			case RASTER_ROW_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::INTEGER(tile_y));
				break;
			case RASTER_WIDTH_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::INTEGER(size_x));
				break;
			case RASTER_HEIGHT_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, Value::INTEGER(size_y));
				break;
			case RASTER_METADATA_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, bind_data.metadata_ds);
				break;
			default: {
				const int num_bands = dataset->GetRasterCount();

				// Write band data as columns...
				if (static_cast<int>(dim_index) < RASTER_FIRST_BAND_COLUMN_INDEX + num_bands) {
					const GDALDataType &data_type = bind_data.data_type;
					const int data_size = GDALGetDataTypeSizeBytes(data_type);
					const CompressionAlg::Value &compression = bind_data.compression_alg;
					const double &nodata_value = bind_data.nodata_value;
					const bool datacube = bind_data.datacube;

					idx_t data_length = sizeof(TileHeader);
					if (datacube) {
						data_length += data_size * size_x * size_y * num_bands;
					} else {
						data_length += data_size * size_x * size_y;
					}

					// Allocate the buffer if it hasn't been allocated yet.
					data_buffer.SetPosition(0);
					data_buffer.GrowCapacity(data_length);

					// Write the header of the data band[s].
					TileHeader header = {
					    compression, static_cast<uint8_t>(data_type), datacube ? num_bands : 1, size_x, size_y,
					    nodata_value};
					data_buffer.WriteData(const_data_ptr_t(&header), sizeof(TileHeader));

					// For datacube, return one unique N-dimensional column with all bands interleaved,
					// Otherwise each band is returned as a separate column.
					data_ptr_t data_ptr = data_buffer.GetData() + sizeof(TileHeader);
					if (datacube) {
						// Read the data of all bands...
						if (dataset->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_ptr, size_x, size_y,
						                      data_type, num_bands, nullptr, 0, 0, 0, nullptr) != CE_None) {
							const std::string error = RasterUtils::GetLastGdalErrorMsg();
							RASTER_SCAN_DEBUG_LOG(1, "Failed to read tile (%d, %d): %s", tile_x, tile_y, error.c_str());
							output.data[col_idx].SetValue(row_index, Value::BLOB(""));
							continue;
						}
					} else {
						const int band_index = static_cast<int>(dim_index - RASTER_FIRST_BAND_COLUMN_INDEX);
						GDALRasterBand *band = dataset->GetRasterBand(band_index + 1);

						// Read the data of the band...
						if (band->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_ptr, size_x, size_y,
						                   data_type, 0, 0, nullptr) != CE_None) {
							const std::string error = RasterUtils::GetLastGdalErrorMsg();
							RASTER_SCAN_DEBUG_LOG(1, "Failed to read tile (%d, %d): %s", tile_x, tile_y, error.c_str());
							output.data[col_idx].SetValue(row_index, Value::BLOB(""));
							continue;
						}
					}
					data_buffer.SetPosition(0);

					// Write the tile data as a blob.
					Value data_value = RasterUtils::TileAsBlob(header, data_buffer, data_length);
					output.data[col_idx].SetValue(row_index, data_value);
				} else {
					throw IOException("Invalid column index: %ld", dim_index);
				}
				break;
			}
			}
		}
		return true;
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<BindData>();
		auto &gstate = input.global_state->Cast<State>();

		// If we've already read all the rows/tiles, return an empty chunk to indicate we're done.
		const idx_t start_row = bind_data.row_offset + gstate.current_row;
		if (start_row >= bind_data.row_count) {
			output.SetCardinality(0);
			return;
		}

		// Calculate how many record we can fit in the output, and tile coordinates to read.

		const idx_t vector_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - start_row);
		idx_t row_id = bind_data.row_offset + gstate.current_rid;
		idx_t output_size = 0;

		const int &tiles_x = bind_data.tiles_x;
		const int &tiles_y = bind_data.tiles_y;
		const int start_ty = static_cast<int>(start_row / tiles_x);
		const int start_tx = static_cast<int>(start_row % tiles_x);

		// RASTER_SCAN_DEBUG_LOG(1, "Execute: start_row=%ld, vector_size=%ld", start_row, vector_size);

		// Loop over the tiles and fill the output chunk.

		MemoryStream data_buffer(Allocator::Get(context));

		for (int tile_y = start_ty; tile_y < tiles_y && output_size < vector_size; ++tile_y) {
			const int first_tx = (tile_y == start_ty) ? start_tx : 0;

			for (int tile_x = first_tx; tile_x < tiles_x && output_size < vector_size; ++tile_x) {
				// Process one tile.
				if (FillOutput(bind_data, row_id, output_size, 0, tile_x, tile_y, data_buffer, output)) {
					output_size++;
				}
				gstate.current_rid++;
				row_id++;
			}
		}

		// Update the row index.
		gstate.current_row += gstate.current_rid;

		// Set the cardinality of the output.
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Replacement Scan
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<TableRef> ReplacementScan(ClientContext &, ReplacementScanInput &input,
	                                            optional_ptr<ReplacementScanData>) {
		auto &table_name = input.table_name;
		auto lower_name = StringUtil::Lower(table_name);

		// Check if the file name ends with some common raster file extensions
		if (StringUtil::EndsWith(lower_name, ".img") || StringUtil::EndsWith(lower_name, ".tiff") ||
		    StringUtil::EndsWith(lower_name, ".tif") || StringUtil::EndsWith(lower_name, ".vrt")) {
			auto table_function = make_uniq<TableFunctionRef>();
			vector<unique_ptr<ParsedExpression>> children;
			children.push_back(make_uniq<ConstantExpression>(Value(table_name)));
			table_function->function = make_uniq<FunctionExpression>("RT_Read", std::move(children));

			return std::move(table_function);
		}
		// else not something we can replace
		return nullptr;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
	    Read and import a variety of geospatial raster file formats using the GDAL library.

	    The `RT_Read` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading raster data from a variety of geospatial raster file formats as if they were DuckDB tables.

	    > See [RT_Drivers](#rt_drivers) for a list of supported file formats and drivers.

	    Except for the `path` parameter, all parameters are optional.

	    | Parameter | Type | Description |
	    | --------- | -----| ----------- |
	    | `path` | VARCHAR | The path to the file to read. Mandatory |
	    | `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. |
	    | `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. |
	    | `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. |

	    Note that GDAL is single-threaded, so this table function will not be able to make full use of parallelism.

	    By using `RT_Read`, the spatial extension also provides “replacement scans” for common geospatial file formats, allowing you to query files of these formats as if they were tables directly.

	    ```sql
	    SELECT * FROM './path/to/some/shapefile/dataset.tif';
	    ```

	    In practice this is just syntax-sugar for calling RT_Read, so there is no difference in performance. If you want to pass additional options, you should use the RT_Read table function directly.

	    The following formats are currently recognized by their file extension:

		| Format | Extension |
		| ------ | --------- |
		| GeoTiff COG | .tif, .tiff |
		| Erdas Imagine | .img |
		| GDAL Virtual | .vrt |
	)";

	static constexpr auto EXAMPLE = R"(
		-- Read a Gtiff file
		SELECT * FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "table");

		TableFunction func("RT_Read", {LogicalType::VARCHAR}, Execute, Bind, Init);
		func.cardinality = Cardinality;

		// Optional parameters
		func.named_parameters["open_options"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["allowed_drivers"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["sibling_files"] = LogicalType::LIST(LogicalType::VARCHAR);
		func.named_parameters["compression"] = LogicalType::VARCHAR;
		func.named_parameters["blocksize_x"] = LogicalType::INTEGER;
		func.named_parameters["blocksize_y"] = LogicalType::INTEGER;
		func.named_parameters["datacube"] = LogicalType::BOOLEAN;

		// Enable projection pushdown - allows DuckDB to tell us which columns are needed
		// The column_ids will be passed to InitGlobal via TableFunctionInitInput
		func.projection_pushdown = true;

		// Enable complex filter pushdown - handles expressions like (A AND B) OR (C AND D)
		// that cannot be represented as simple TableFilter objects
		func.pushdown_complex_filter = PushdownComplexFilter;

		RegisterFunction<TableFunction>(loader, func, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);

		// Replacement scan
		auto &db = loader.GetDatabaseInstance();
		auto &config = DBConfig::GetConfig(db);
		config.replacement_scans.emplace_back(ReplacementScan);

		// Register optimizer extension for LIMIT pushdown
		OptimizerExtension raster_optimizer;
		raster_optimizer.optimize_function = RT_Read::Optimize;
		OptimizerExtension::Register(config, std::move(raster_optimizer));
	}
};

} // namespace

// #####################################################################################################################
// Register Read Function
// #####################################################################################################################

void RasterReadFunction::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Read::Register(loader);
}

} // namespace duckdb
