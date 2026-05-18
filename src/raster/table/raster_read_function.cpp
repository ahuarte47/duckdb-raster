#include "raster_read_function.hpp"
#include "raster_utils.hpp"
#include "filter_eval.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"
#include <sstream>

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

// GDAL
#include "gdal_priv.h"
#include "gdal_utils.h"
#include "modules/gdal_file_system.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// Utilities
//======================================================================================================================

//! Resolve a file path into a GDAL-accessible path.
static std::string GdalFilePath(const std::string &file_path, ClientContext &context) {
	// Remote paths are routed through DuckDB's custom VSI handler by prepending the registered
	// DuckDBFileSystemPrefix, so that GDAL I/O calls are transparently forwarded to DuckDB's
	// own file system (e.g. HTTP, S3, Azure, ...).
	// Other paths are returned unchanged for GDAL to handle them directly.
	if (FileSystem::IsRemoteFile(file_path)) {
		const auto &file_prefix = DuckDBFileSystemPrefix::GetOrCreate(context);
		const auto prefixed_path = file_prefix.AddPrefix(file_path);
		RASTER_SCAN_DEBUG_LOG(1, "Prefix remote file path: '%s' to '%s'", file_path.c_str(), prefixed_path.c_str());
		return prefixed_path;
	}
	return file_path;
}

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

//! Convert a fileset parameter map to an array suitable for passing to GDAL functions.
static std::vector<char const *> FilesetParametersAsVector(const named_parameter_map_t &input,
                                                           const std::string &keyname, ClientContext &context) {
	auto output = std::vector<char const *>();

	auto input_param = input.find(keyname);
	if (input_param != input.end()) {
		output.reserve(input.size() + 1);

		for (auto &param : ListValue::GetChildren(input_param->second)) {
			const auto file_path = StringValue::Get(param);
			output.push_back(GdalFilePath(file_path, context).c_str());
		}
		output.push_back(nullptr);
	}
	return output;
}

//! Open a GDAL dataset from a list of file names and named parameters.
static GDALDataset *OpenDataset(ClientContext &context, std::vector<std::string> &file_names,
                                const named_parameter_map_t &params) {
	GDALDataset *dataset = nullptr;

	if (file_names.size() == 1) {
		std::string file_path = file_names[0];

		const auto gdal_options = NamedParametersAsVector(params, "open_options");
		const auto gdal_drivers = NamedParametersAsVector(params, "allowed_drivers");
		const auto gdal_sibling = FilesetParametersAsVector(params, "sibling_files", context);

		dataset = GDALDataset::Open(file_path.c_str(), GDAL_OF_RASTER | GDAL_OF_VERBOSE_ERROR,
		                            gdal_drivers.empty() ? nullptr : gdal_drivers.data(),
		                            gdal_options.empty() ? nullptr : gdal_options.data(),
		                            gdal_sibling.empty() ? nullptr : gdal_sibling.data());

		if (!dataset) {
			const std::string error = RasterUtils::GetLastGdalErrorMsg();
			throw IOException("Could not open file: " + file_path + " (" + error + ")");
		}
		RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset opened: '%s'", file_path.c_str());

	} else {
		bool separate_bands = false;

		if (params.find("separate_bands") != params.end()) {
			separate_bands = params.at("separate_bands").GetValue<bool>();
		}

		// Build a VRT mosaic from the input files using GDAL's in-memory filesystem.

		std::string vrt_path = "/vsimem/" + UUID::ToString(UUID::GenerateRandomUUID()) + ".vrt";
		std::vector<std::string> vrt_args = {"-r", "nearest"};

		if (separate_bands) {
			vrt_args.push_back("-separate");
		}

		std::vector<const char *> vrt_argv;
		for (const auto &s : vrt_args) {
			vrt_argv.push_back(s.c_str());
		}
		vrt_argv.push_back(nullptr);

		using GDALBuildVRTOptionsPtr = std::unique_ptr<GDALBuildVRTOptions, decltype(&GDALBuildVRTOptionsFree)>;

		GDALBuildVRTOptionsPtr vrt_opts(GDALBuildVRTOptionsNew(const_cast<char **>(vrt_argv.data()), nullptr),
		                                GDALBuildVRTOptionsFree);

		std::vector<const char *> file_paths;
		for (const auto &fn : file_names) {
			file_paths.push_back(fn.c_str());
		}

		dataset = GDALDataset::FromHandle(GDALBuildVRT(vrt_path.c_str(), static_cast<int>(file_paths.size()), nullptr,
		                                               file_paths.data(), vrt_opts.get(), nullptr));

		if (!dataset) {
			const std::string error = RasterUtils::GetLastGdalErrorMsg();
			throw IOException("Failed to build VRT mosaic from input files (" + error + ")");
		}
		RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset opened: '%s'", "<multiple files>");
	}
	return dataset;
}

//======================================================================================================================
// RT_Read
//======================================================================================================================

struct RT_Read {
	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData final : TableFunctionData {
		std::vector<std::string> file_names;
		named_parameter_map_t parameters;
		DataFormat::Value data_format = DataFormat::Value::RAW;
		bool skip_empty_tiles = true;
		bool make_datacube = false;

		GDALDatasetUniquePtr dataset;
		std::string metadata_ds;
		std::string crs;
		double geo_transform[6] = {0};
		GDALDataType data_type = GDT_Unknown;
		double nodata_value = NumericLimits<double>::Minimum();
		int32_t raster_size_x = 0;
		int32_t raster_size_y = 0;
		int32_t block_size_x = 0;
		int32_t block_size_y = 0;
		int32_t tiles_x = 0;
		int32_t tiles_y = 0;

		vector<LogicalType> column_types;
		idx_t row_offset = 0;
		idx_t row_count = 0;

		// Pushdown filter expressions.
		vector<std::unique_ptr<Expression>> filter_expressions;

		~BindData() override {
			// Ensure the GDAL dataset is properly closed when the bind data is destroyed.
			dataset.reset();

			RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset closed: '%s'",
			                      file_names.size() > 1 ? "<multiple files>" : file_names[0].c_str());
		}
	};

	static unique_ptr<FunctionData> BindUniqueFile(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names) {
		std::vector<std::string> file_names;

		const auto file_path = input.inputs[0].GetValue<std::string>();
		if (file_path.find("*") != std::string::npos) {
			auto &fs = FileSystem::GetFileSystem(context);

			auto files = fs.GlobFiles(file_path, FileGlobOptions::DISALLOW_EMPTY);
			for (auto &file : files) {
				file_names.push_back(GdalFilePath(file.path, context));
			}
		} else {
			file_names.push_back(GdalFilePath(file_path, context));
		}

		return Bind(context, input, return_types, names, file_names);
	}

	static unique_ptr<FunctionData> BindMultiFile(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names) {
		std::vector<std::string> file_names;

		if (input.inputs.empty()) {
			throw InvalidInputException("Expected a list of file paths as input");
		}
		if (input.inputs[0].type().id() != LogicalTypeId::LIST) {
			throw InvalidInputException("Expected a list of file paths for the first argument");
		}
		for (const auto &child : ListValue::GetChildren(input.inputs[0])) {
			const auto file_path = StringValue::Get(child);

			if (file_path.find("*") != std::string::npos) {
				throw InvalidInputException(
				    "File globbing is not supported when passing multiple file paths as a list.");
			}
			file_names.push_back(GdalFilePath(file_path, context));
		}
		return Bind(context, input, return_types, names, file_names);
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names,
	                                     std::vector<std::string> &file_names) {
		if (file_names.empty()) {
			throw InvalidInputException("No input files provided.");
		}

		const auto &params = input.named_parameters;

		DataFormat::Value data_format = DataFormat::Value::RAW;
		if (params.find("data_format") != params.end()) {
			data_format = DataFormat::FromString(params.at("data_format").GetValue<string>());
		}

		bool make_datacube = false;
		if (params.find("datacube") != params.end()) {
			make_datacube = params.at("datacube").GetValue<bool>();
		}

		bool skip_empty_tiles = true;
		if (params.find("skip_empty_tiles") != params.end()) {
			skip_empty_tiles = params.at("skip_empty_tiles").GetValue<bool>();
		}

		// Open the dataset.

		GDALDatasetUniquePtr dataset = GDALDatasetUniquePtr(OpenDataset(context, file_names, params));

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
			block_size_x = params.at("blocksize_x").GetValue<int>();
		}
		if (params.find("blocksize_y") != params.end()) {
			block_size_y = params.at("blocksize_y").GetValue<int>();
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
		RASTER_SCAN_DEBUG_LOG(1, " > Data format: '%s'", DataFormat::ToString(data_format).c_str());

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
			} else if (!std::isnan(nodata) && !std::isnan(nodata_value) && nodata != nodata_value) {
				throw IOException("All bands must have the same nodata value");
			}

			// For datacubes, we return a single BLOB column containing all the bands.
			if (!make_datacube || b == 1) {
				band_name = make_datacube ? "datacube" : StringUtil::Format("databand_%d", b);
				names.emplace_back(band_name);
				return_types.emplace_back(RasterTypes::DATACUBE());
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
		metadata_ds << "\"data_format\": \"" << DataFormat::ToString(data_format) << "\", ";
		metadata_ds << "\"datacube\": " << (make_datacube ? "true" : "false") << ", ";
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
		result->file_names = std::move(file_names);
		result->parameters = params;
		result->data_format = data_format;
		result->skip_empty_tiles = skip_empty_tiles;
		result->make_datacube = make_datacube;
		result->dataset = std::move(dataset);
		result->metadata_ds = metadata_ds.str();
		result->crs = std::move(crs);
		std::copy(std::begin(gt), std::end(gt), std::begin(result->geo_transform));
		result->data_type = data_type;
		result->nodata_value = nodata_value;
		result->raster_size_x = raster_size_x;
		result->raster_size_y = raster_size_y;
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
		idx_t current_row;
		explicit State() : current_row(0) {
		}
	};

	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		// Capture the final projected column IDs here, after all optimizer passes.
		// input.column_ids is guaranteed to match output.data.size() in Execute.
		auto &bind_data = const_cast<BindData &>(input.bind_data->Cast<BindData>());
		bind_data.column_ids = input.column_ids;

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

					if (StringUtil::Lower(get.function.name) == "rt_read" ||
					    StringUtil::Lower(get.function.name) == "rt_readcells") {
						auto &bind_data = get.bind_data->Cast<BindData>();

						if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							const idx_t offset_value = limit.offset_val.GetConstantValue();
							RASTER_SCAN_DEBUG_LOG(1, "OFFSET pushdown: %" PRIu64, offset_value);
							bind_data.row_offset = offset_value;
							limit.offset_val = BoundLimitNode();
						}
						const idx_t limit_value = limit.limit_val.GetConstantValue();
						RASTER_SCAN_DEBUG_LOG(1, "LIMIT pushdown: %" PRIu64, limit_value);
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

		// Catch filter expressions for later evaluation during scanning if possible.
		if (!expressions.empty()) {
			vector<std::unique_ptr<Expression>> temp_expressions;
			bool do_pushdown = true;

			for (const auto &expr : expressions) {
				if (!do_pushdown) {
					break;
				}
				auto expr_copy = expr->Copy();

				// We need to convert the column references in the filter expressions from BoundColumnRefExpression
				// to BoundReferenceExpression, so that one ExpressionExecutor can execute them during scanning.
				// Also, we check if the filter expressions reference any BLOB data band columns, we only want
				// to prefilter tiles on "small" columns without preloading the entire "big" BLOBs.
				ExpressionIterator::VisitExpressionClassMutable(
				    expr_copy, ExpressionClass::BOUND_COLUMN_REF, [&do_pushdown](unique_ptr<Expression> &child) {
					    if (do_pushdown) {
						    const auto &col_ref = child->Cast<BoundColumnRefExpression>();
						    const auto &type_id = col_ref.return_type.id();

						    // If filter expressions reference BLOBs, we totally delegate the filter to DuckDB.
						    if (type_id == LogicalTypeId::BLOB || type_id == RasterTypes::DATACUBE().id()) {
							    do_pushdown = false;
							    return;
						    }

						    const auto &column_alias = col_ref.GetAlias();
						    const auto &column_index = col_ref.binding.column_index;
						    const auto &return_type = col_ref.return_type;
						    child = make_uniq<BoundReferenceExpression>(column_alias, return_type, column_index);
					    }
				    });

				temp_expressions.push_back(std::move(expr_copy));
			}
			if (do_pushdown) {
				bind_data.filter_expressions = std::move(temp_expressions);
			}
		}
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

		// This is an estimate of the number of rows/tiles
		result->has_estimated_cardinality = true;
		result->estimated_cardinality = result->max_cardinality;

		return result;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Fill the output chunk with data for the specified tile coordinates.
	static bool FillOutput(const BindData &bind_data, const idx_t &row_id, const idx_t &row_index, const int32_t &level,
	                       const int32_t &tile_x, const int32_t &tile_y, const FilterContext &filter_context,
	                       DataCube &data_cube, DataChunk &output) {
		GDALDataset *dataset = bind_data.dataset.get();

		const auto &metadata_ds = bind_data.metadata_ds;
		const auto &gt = bind_data.geo_transform;
		const int32_t raster_size_x = bind_data.raster_size_x;
		const int32_t raster_size_y = bind_data.raster_size_y;
		const int32_t block_size_x = bind_data.block_size_x;
		const int32_t block_size_y = bind_data.block_size_y;
		const auto &column_ids = bind_data.column_ids;

		const int32_t offset_x = tile_x * block_size_x;
		const int32_t offset_y = tile_y * block_size_y;
		const int32_t size_x = MinValue<int32_t>(block_size_x, raster_size_x - offset_x);
		const int32_t size_y = MinValue<int32_t>(block_size_y, raster_size_y - offset_y);

		// Skip empty/sparse tiles (SPARSE_OK GeoTIFF optimization).
		// GetDataCoverageStatus() returns GDAL_DATA_COVERAGE_STATUS_EMPTY when the tile
		// has no physical data in the file. GDAL_DATA_COVERAGE_STATUS_UNIMPLEMENTED means
		// the driver does not support the check, so we proceed normally in that case.
		if (bind_data.skip_empty_tiles) {
			GDALRasterBand *band_1 = dataset->GetRasterBand(1);
			const int cov = band_1->GetDataCoverageStatus(offset_x, offset_y, size_x, size_y, 0, nullptr);

			if (!(cov & GDAL_DATA_COVERAGE_STATUS_UNIMPLEMENTED) && !(cov & GDAL_DATA_COVERAGE_STATUS_DATA)) {
				RASTER_SCAN_DEBUG_LOG(3, " > txy=(%d, %d): empty sparse tile, skipped", tile_x, tile_y);
				return false;
			}
		}

		const Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x, offset_y);
		const Point2D pt1 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x + size_x, offset_y);
		const Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x + size_x, offset_y + size_y);
		const Point2D pt3 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x, offset_y + size_y);

		const double x_min = MinValue<double>(MinValue<double>(pt0.x, pt1.x), MinValue<double>(pt2.x, pt3.x));
		const double y_min = MinValue<double>(MinValue<double>(pt0.y, pt1.y), MinValue<double>(pt2.y, pt3.y));
		const double x_max = MaxValue<double>(MaxValue<double>(pt0.x, pt1.x), MaxValue<double>(pt2.x, pt3.x));
		const double y_max = MaxValue<double>(MaxValue<double>(pt0.y, pt1.y), MaxValue<double>(pt2.y, pt3.y));

		Value bbox = Value::STRUCT({{"xmin", x_min}, {"ymin", y_min}, {"x_max", x_max}, {"ymax", y_max}});
		bbox.Reinterpret(RasterTypes::BBOX());

		const std::string geometry_wkt =
		    StringUtil::Format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", pt0.x, pt0.y, pt1.x, pt1.y, pt2.x,
		                       pt2.y, pt3.x, pt3.y, pt0.x, pt0.y);

		const RasterRow raster_row {Value::BIGINT(row_id),
		                            Value::DOUBLE(pt0.x + 0.5 * (pt2.x - pt0.x)),
		                            Value::DOUBLE(pt0.y + 0.5 * (pt2.y - pt0.y)),
		                            bbox,
		                            Value(geometry_wkt),
		                            Value::INTEGER(level),
		                            Value::INTEGER(tile_x),
		                            Value::INTEGER(tile_y),
		                            Value::INTEGER(size_x),
		                            Value::INTEGER(size_y),
		                            metadata_ds};

		// The filter expressions were evaluated but raster tile does not match the conditions?
		if (!FilterEval::Eval(raster_row, filter_context)) {
			RASTER_SCAN_DEBUG_LOG(3, " > txy=(%d, %d): tile did not match filter conditions, skipped", tile_x, tile_y);
			return false;
		}

		// RASTER_SCAN_DEBUG_LOG(3, " > txy=(%d, %d): t_coords=(%d, %d, %d, %d), bbox=(%f, %f, %f, %f)", tile_x, tile_y,
		//                       offset_x, offset_y, size_x, size_y, x_min, y_min, x_max, y_max);

		// Fill the output chunk for the current row.
		for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
			const auto &dim_index = column_ids[col_idx];

			switch (dim_index) {
			case RASTER_ROWID_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.row_id);
				break;
			case RASTER_X_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.x);
				break;
			case RASTER_Y_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.y);
				break;
			case RASTER_BBOX_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.bbox);
				break;
			case RASTER_GEOMETRY_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.geometry);
				break;
			case RASTER_LEVEL_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.level);
				break;
			case RASTER_COL_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.tile_x);
				break;
			case RASTER_ROW_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.tile_y);
				break;
			case RASTER_WIDTH_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.size_x);
				break;
			case RASTER_HEIGHT_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, raster_row.size_y);
				break;
			case RASTER_METADATA_COLUMN_INDEX:
				output.data[col_idx].SetValue(row_index, metadata_ds);
				break;
			default: {
				const int32_t num_bands = dataset->GetRasterCount();

				// Write band data as columns...
				if (dim_index < static_cast<uint64_t>(RASTER_FIRST_BAND_COLUMN_INDEX + num_bands)) {
					const GDALDataType &data_type = bind_data.data_type;
					const DataFormat::Value &data_format = bind_data.data_format;
					const double &nodata_value = bind_data.nodata_value;
					const bool make_datacube = bind_data.make_datacube;

					// Write the header of the data band[s].

					DataHeader header = {DataFormat::Value::RAW,
					                     RasterUtils::GdalTypeToDataType(data_type),
					                     make_datacube ? num_bands : 1,
					                     size_x,
					                     size_y,
					                     nodata_value};

					data_cube.SetHeader(header, true);
					MemoryStream &data_buffer = data_cube.GetBuffer();

					// For datacube, return one unique N-dimensional column with all bands interleaved,
					// Otherwise each band is returned as a separate column.
					data_ptr_t data_ptr = data_buffer.GetData() + sizeof(DataHeader);
					CPLErr read_err = CE_None;
					if (make_datacube) {
						// Read the data of all bands...
						read_err = dataset->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_ptr, size_x,
						                             size_y, data_type, num_bands, nullptr, 0, 0, 0, nullptr);

					} else {
						const int band_index = static_cast<int>(dim_index - RASTER_FIRST_BAND_COLUMN_INDEX);
						GDALRasterBand *band = dataset->GetRasterBand(band_index + 1);

						// Read the data of the band...
						read_err = band->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_ptr, size_x, size_y,
						                          data_type, 0, 0, nullptr);
					}

					// Is there an error reading the tile data?
					if (read_err != CE_None) {
						const std::string error = RasterUtils::GetLastGdalErrorMsg();
						RASTER_SCAN_DEBUG_LOG(1, "Failed to read tile (%d, %d): %s", tile_x, tile_y, error.c_str());
						output.data[col_idx].SetValue(row_index, DataCube::EMPTY_CUBE(header.data_type).ToBlob());
						continue;
					}

					data_buffer.SetPosition(data_cube.GetExpectedSizeBytes());

					// Write the tile data as a BLOB column.
					if (data_format != DataFormat::Value::RAW) {
						data_cube.ChangeFormat(data_format);
					}
					output.data[col_idx].SetValue(row_index, data_cube.ToBlob());
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
		idx_t row_id = bind_data.row_offset + gstate.current_row;
		idx_t output_size = 0;

		const int32_t &tiles_x = bind_data.tiles_x;
		const int32_t &tiles_y = bind_data.tiles_y;
		const int32_t start_ty = static_cast<int32_t>(start_row / tiles_x);
		const int32_t start_tx = static_cast<int32_t>(start_row % tiles_x);

		RASTER_SCAN_DEBUG_LOG(2, "Execute: start_row=%" PRIu64 ", vector_size=%" PRIu64, start_row, vector_size);

		const auto &filter_expressions = bind_data.filter_expressions;
		const auto &column_ids = bind_data.column_ids;
		const auto &column_types = bind_data.column_types;
		const FilterContext filter_context(context, filter_expressions, column_ids, column_types);

		// Loop over the tiles and fill the output chunk, it reuses DataCubes for avoiding repeated memory allocations.

		DataCube data_cube(Allocator::Get(context));

		for (int32_t tile_y = start_ty; tile_y < tiles_y && output_size < vector_size; ++tile_y) {
			const int32_t first_tx = (tile_y == start_ty) ? start_tx : 0;

			for (int32_t tile_x = first_tx; tile_x < tiles_x && output_size < vector_size; ++tile_x) {
				// Process one tile.
				if (FillOutput(bind_data, row_id, output_size, 0, tile_x, tile_y, filter_context, data_cube, output)) {
					output_size++;
				}
				row_id++;
			}
		}

		// Update the row index.
		gstate.current_row = (row_id - bind_data.row_offset);

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
	// Progress Scan
	//------------------------------------------------------------------------------------------------------------------

	static double Progress(ClientContext &context, const FunctionData *bind_data_p,
	                       const GlobalTableFunctionState *global_state) {
		auto &bind_data = bind_data_p->Cast<BindData>();
		auto &gstate = global_state->Cast<State>();

		// The table is empty, no progress to report.
		if (bind_data.row_count == 0) {
			return 100;
		}

		auto p = 100 * (static_cast<double>(gstate.current_row) / static_cast<double>(bind_data.row_count));
		return p > 100 ? 100 : p;
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Open a raster file (or a mosaic of raster files) and return a table with the raster data.

		The `RT_Read` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading raster data from a variety of geospatial raster file formats as if they were DuckDB tables.

		> See [RT_Drivers](#rt_drivers) for a list of supported file formats and drivers.

		The `RT_Read` function accepts parameters, most of them optional:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `path` | VARCHAR | The path to the file to read. The only mandatory parameter. |
		| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. Refer to the GDAL documentation for available options. Only for single-file version of the function. |
		| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. Only for single-file version of the function. |
		| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. Only for single-file version of the function. |
		| `separate_bands` | BOOLEAN | `true` means that each input goes into a separate band in the VRT dataset. Otherwise, the files are considered as source rasters of a larger mosaic and the VRT file has the same number of bands as the input files. Only for multi-file version of the function. `false` is the default. |
		| `data_format` | VARCHAR | Compression format used when packing the pixel data into the BLOB. See the data format table in the BLOB structure section below. `RAW` (uncompressed) is the default. |
		| `blocksize_x` | INTEGER | The block size of the tile in the x direction. You can use this parameter to override the original block size of the raster. |
		| `blocksize_y` | INTEGER | The block size of the tile in the y direction. You can use this parameter to override the original block size of the raster. |
		| `skip_empty_tiles` | BOOLEAN | When `true`, tiles that contain no data are omitted from the output (checks the `GDAL_DATA_COVERAGE_STATUS_DATA` flag when supported). `true` is the default. |
		| `datacube` | BOOLEAN | When `true`, all bands are merged into a single `datacube` column; otherwise each band is returned as a separate `databand_N` column. `false` is the default. |

		This is the list of columns returned by `RT_Read`:

		+ `id` is a unique identifier for each tile of the raster.
		+ `x` and `y` are the coordinates of the center of each tile. The coordinate reference system is the same as the one of the raster file.
		+ `bbox` is the bounding box of each tile, which is a struct with `xmin`, `ymin`, `xmax`, and `ymax` fields.
		+ `geometry` is the footprint of each tile as a polygon.
		+ `level`, `tile_x`, and `tile_y` are the tile grid coordinates. The raster is partitioned into tiles of `blocksize_x` × `blocksize_y` pixels (or the file's native block size when not overridden).
		+ `cols` and `rows` are the actual pixel dimensions of the tile, which may differ from the requested block size at the edges of the raster.
		+ `metadata` is a JSON column with the raster file metadata: band properties (data type, nodata value, etc.), spatial reference system, geotransform, and any driver-specific metadata.
		+ `databand_1`, `databand_2`, … are BLOB columns, each holding the pixel data for one raster band together with a small binary header that describes the tile layout. When the `datacube` option is `true`, a single `datacube` column is returned instead, containing all bands in the same BLOB format.

		Note that GDAL is single-threaded, so this table function cannot fully exploit DuckDB parallelism.

		The raster extension also provides "replacement scans" for common raster file formats, allowing you to query these files as if they were tables:

		```sql
		SELECT * FROM './path/to/some/file.tif';
		```

		This is syntax sugar for `RT_Read`. To pass additional options, use `RT_Read` directly.

		The following formats are recognised by their file extension:

		| Format | Extension |
		| ------ | --------- |
		| GeoTIFF / COG | .tif, .tiff |
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

		// Configure the functions and register them in the function set.

		TableFunctionSet function_set("RT_Read");

		TableFunction func_01("RT_Read", {LogicalType::VARCHAR}, Execute, BindUniqueFile, Init);
		func_01.named_parameters["open_options"] = LogicalType::LIST(LogicalType::VARCHAR);
		func_01.named_parameters["allowed_drivers"] = LogicalType::LIST(LogicalType::VARCHAR);
		func_01.named_parameters["sibling_files"] = LogicalType::LIST(LogicalType::VARCHAR);

		TableFunction func_02("RT_Read", {LogicalType::LIST(LogicalType::VARCHAR)}, Execute, BindMultiFile, Init);
		func_02.named_parameters["separate_bands"] = LogicalType::BOOLEAN;

		for (auto *func : {&func_01, &func_02}) {
			func->cardinality = Cardinality;
			func->table_scan_progress = Progress;

			// Common optional parameters
			func->named_parameters["data_format"] = LogicalType::VARCHAR;
			func->named_parameters["blocksize_x"] = LogicalType::INTEGER;
			func->named_parameters["blocksize_y"] = LogicalType::INTEGER;
			func->named_parameters["skip_empty_tiles"] = LogicalType::BOOLEAN;
			func->named_parameters["datacube"] = LogicalType::BOOLEAN;

			// Enable projection pushdown - allows DuckDB to tell us which columns are needed
			// The column_ids will be passed to InitGlobal via TableFunctionInitInput
			func->projection_pushdown = true;

			// Enable complex filter pushdown - handles expressions like (A AND B) OR (C AND D)
			// that cannot be represented as simple TableFilter objects
			func->pushdown_complex_filter = PushdownComplexFilter;

			function_set.AddFunction(*func);
		}

		RegisterFunction<TableFunctionSet>(loader, function_set, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION,
		                                   EXAMPLE, tags);

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

//======================================================================================================================
// RT_ReadCells
//======================================================================================================================

struct RT_ReadCells {
	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<FunctionData> BindUniqueFile(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names) {
		std::vector<std::string> file_names;

		const auto file_path = input.inputs[0].GetValue<std::string>();
		if (file_path.find("*") != std::string::npos) {
			auto &fs = FileSystem::GetFileSystem(context);

			auto files = fs.GlobFiles(file_path, FileGlobOptions::DISALLOW_EMPTY);
			for (auto &file : files) {
				file_names.push_back(GdalFilePath(file.path, context));
			}
		} else {
			file_names.push_back(GdalFilePath(file_path, context));
		}

		return Bind(context, input, return_types, names, file_names);
	}

	static unique_ptr<FunctionData> BindMultiFile(ClientContext &context, TableFunctionBindInput &input,
	                                              vector<LogicalType> &return_types, vector<string> &names) {
		std::vector<std::string> file_names;

		if (input.inputs.empty()) {
			throw InvalidInputException("Expected a list of file paths as input");
		}
		if (input.inputs[0].type().id() != LogicalTypeId::LIST) {
			throw InvalidInputException("Expected a list of file paths for the first argument");
		}
		for (const auto &child : ListValue::GetChildren(input.inputs[0])) {
			const auto file_path = StringValue::Get(child);

			if (file_path.find("*") != std::string::npos) {
				throw InvalidInputException(
				    "File globbing is not supported when passing multiple file paths as a list.");
			}
			file_names.push_back(GdalFilePath(file_path, context));
		}
		return Bind(context, input, return_types, names, file_names);
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, TableFunctionBindInput &input,
	                                     vector<LogicalType> &return_types, vector<string> &names,
	                                     std::vector<std::string> &file_names) {
		if (file_names.empty()) {
			throw InvalidInputException("No input files provided.");
		}

		const auto &params = input.named_parameters;

		// Open the dataset.

		GDALDatasetUniquePtr dataset = GDALDatasetUniquePtr(OpenDataset(context, file_names, params));

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

		// Calculate the total number of rows in the dataset (i.e. number of cells).

		const int raster_size_x = dataset->GetRasterXSize();
		const int raster_size_y = dataset->GetRasterYSize();

		int block_size_x, block_size_y;
		GDALRasterBand *band = dataset->GetRasterBand(1);
		band->GetBlockSize(&block_size_x, &block_size_y);

		if (block_size_x <= 0 || block_size_y <= 0) {
			throw InvalidInputException("Invalid block size specified, must be greater than 0");
		}

		const int tiles_x = static_cast<int>((raster_size_x + block_size_x - 1) / block_size_x);
		const int tiles_y = static_cast<int>((raster_size_y + block_size_y - 1) / block_size_y);
		const int row_count = raster_size_x * raster_size_y;

		RASTER_SCAN_DEBUG_LOG(1, " > Raster size: %d x %d", raster_size_x, raster_size_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Block size: %d x %d", block_size_x, block_size_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Tiles: %d x %d", tiles_x, tiles_y);
		RASTER_SCAN_DEBUG_LOG(1, " > Total rows (cells): %d", row_count);

		// Fill the column definitions.

		names.emplace_back("id");
		return_types.emplace_back(LogicalType::BIGINT);
		names.emplace_back("x");
		return_types.emplace_back(LogicalType::DOUBLE);
		names.emplace_back("y");
		return_types.emplace_back(LogicalType::DOUBLE);
		names.emplace_back("geometry");
		return_types.emplace_back(crs.empty() ? LogicalType::GEOMETRY() : LogicalType::GEOMETRY(crs));
		names.emplace_back("col");
		return_types.emplace_back(LogicalType::INTEGER);
		names.emplace_back("row");
		return_types.emplace_back(LogicalType::INTEGER);

		GDALDataType data_type = GDT_Unknown;
		double nodata_value = NumericLimits<double>::Minimum();

		vector<LogicalType> column_types;
		for (int i = 0; i < CELL_FIRST_BAND_COLUMN_INDEX; i++) {
			column_types.emplace_back(return_types[i]);
		}

		for (int b = 1; b <= dataset->GetRasterCount(); b++) {
			GDALRasterBand *band = dataset->GetRasterBand(b);

			std::string band_name = StringUtil::Format("band_%d", b);
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
			} else if (!std::isnan(nodata) && !std::isnan(nodata_value) && nodata != nodata_value) {
				throw IOException("All bands must have the same nodata value");
			}

			names.emplace_back(band_name);
			return_types.emplace_back(RasterUtils::GdalTypeToLogicalType(raster_type));
		}

		// Return the bind data.

		auto result = make_uniq<RT_Read::BindData>();
		result->file_names = std::move(file_names);
		result->parameters = params;
		result->data_format = DataFormat::RAW;
		result->skip_empty_tiles = true;
		result->make_datacube = false;
		result->dataset = std::move(dataset);
		result->metadata_ds = "{}";
		result->crs = std::move(crs);
		std::copy(std::begin(gt), std::end(gt), std::begin(result->geo_transform));
		result->data_type = data_type;
		result->nodata_value = nodata_value;
		result->raster_size_x = raster_size_x;
		result->raster_size_y = raster_size_y;
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
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Result of filling the output chunk of raster cells.
	struct OutputResult {
		int64_t processed = 0;
		int64_t accepted = 0;
	};

	//! Find the tile coordinates (tile_x, tile_y) for the given global cell id.
	static bool FindTileCoordinates(const RT_Read::BindData &bind_data, const int64_t &cell_id, int32_t &tile_x,
	                                int32_t &tile_y) {
		const int32_t &raster_size_x = bind_data.raster_size_x;
		const int32_t &raster_size_y = bind_data.raster_size_y;
		const int32_t &block_size_x = bind_data.block_size_x;
		const int32_t &block_size_y = bind_data.block_size_y;
		const int32_t &tiles_x = bind_data.tiles_x;
		const int32_t &tiles_y = bind_data.tiles_y;

		int64_t num_cells = 0;

		for (int32_t ty = 0; ty < tiles_y; ++ty) {
			for (int32_t tx = 0; tx < tiles_x; ++tx) {
				const int32_t offset_x = tx * block_size_x;
				const int32_t offset_y = ty * block_size_y;
				const int32_t size_x = MinValue<int32_t>(block_size_x, raster_size_x - offset_x);
				const int32_t size_y = MinValue<int32_t>(block_size_y, raster_size_y - offset_y);

				if (cell_id >= num_cells && cell_id < num_cells + size_x * size_y) {
					tile_x = tx;
					tile_y = ty;
					return true;
				}
				num_cells += size_x * size_y;
			}
		}
		return false;
	}

	//! Find the local cell id within the tile for the given global cell id.
	static bool FindLocalCellId(const RT_Read::BindData &bind_data, const int64_t &cell_id, int32_t &local_cell_id) {
		const int32_t &raster_size_x = bind_data.raster_size_x;
		const int32_t &raster_size_y = bind_data.raster_size_y;
		const int32_t &block_size_x = bind_data.block_size_x;
		const int32_t &block_size_y = bind_data.block_size_y;
		const int32_t &tiles_x = bind_data.tiles_x;
		const int32_t &tiles_y = bind_data.tiles_y;

		int64_t num_cells = 0;

		for (int32_t ty = 0; ty < tiles_y; ++ty) {
			for (int32_t tx = 0; tx < tiles_x; ++tx) {
				const int32_t offset_x = tx * block_size_x;
				const int32_t offset_y = ty * block_size_y;
				const int32_t size_x = MinValue<int32_t>(block_size_x, raster_size_x - offset_x);
				const int32_t size_y = MinValue<int32_t>(block_size_y, raster_size_y - offset_y);

				if (cell_id >= num_cells && cell_id < num_cells + size_x * size_y) {
					local_cell_id = static_cast<int32_t>(cell_id - num_cells);
					return true;
				}
				num_cells += size_x * size_y;
			}
		}
		return false;
	}

	//! Fill the output chunk with data for the specified tile coordinates.
	static OutputResult FillOutput(const RT_Read::BindData &bind_data, const idx_t &row_id, const idx_t &row_index,
	                               const size_t &max_output, const int32_t &tile_x, const int32_t &tile_y,
	                               const FilterContext &filter_context, DataCube &data_cube, DataChunk &output) {
		GDALDataset *dataset = bind_data.dataset.get();

		const auto &gt = bind_data.geo_transform;
		const int32_t raster_size_x = bind_data.raster_size_x;
		const int32_t raster_size_y = bind_data.raster_size_y;
		const int32_t block_size_x = bind_data.block_size_x;
		const int32_t block_size_y = bind_data.block_size_y;
		const auto &column_ids = bind_data.column_ids;

		const int32_t offset_x = tile_x * block_size_x;
		const int32_t offset_y = tile_y * block_size_y;
		const int32_t size_x = MinValue<int32_t>(block_size_x, raster_size_x - offset_x);
		const int32_t size_y = MinValue<int32_t>(block_size_y, raster_size_y - offset_y);

		int32_t local_cell_id = 0;

		if (!FindLocalCellId(bind_data, row_id, local_cell_id)) {
			throw IOException("Failed to find local cell id for row id %" PRIu64, row_id);
		}

		OutputResult result = {0, 0};
		const int32_t count = size_x * size_y - local_cell_id;
		idx_t row_idx = row_index;

		// Skip empty/sparse tiles (SPARSE_OK GeoTIFF optimization).
		// GetDataCoverageStatus() returns GDAL_DATA_COVERAGE_STATUS_EMPTY when the tile
		// has no physical data in the file. GDAL_DATA_COVERAGE_STATUS_UNIMPLEMENTED means
		// the driver does not support the check, so we proceed normally in that case.
		if (bind_data.skip_empty_tiles) {
			GDALRasterBand *band_1 = dataset->GetRasterBand(1);
			const int cov = band_1->GetDataCoverageStatus(offset_x, offset_y, size_x, size_y, 0, nullptr);

			if (!(cov & GDAL_DATA_COVERAGE_STATUS_UNIMPLEMENTED) && !(cov & GDAL_DATA_COVERAGE_STATUS_DATA)) {
				RASTER_SCAN_DEBUG_LOG(3, " > txy=(%d, %d): empty sparse tile, skipped", tile_x, tile_y);
				result.processed += count;
				return result;
			}
		}

		const GDALDataType &data_type = bind_data.data_type;
		const int data_size = GDALGetDataTypeSizeBytes(bind_data.data_type);
		const int32_t num_bands = dataset->GetRasterCount();

		MemoryStream &data_buffer = data_cube.GetBuffer();
		bool data_read = false;

		// For each cell in the tile, evaluate the filter expressions and fill the output chunk.
		for (int32_t i = 0; i < count; i++) {
			const int32_t cell_id = local_cell_id + i;
			const int32_t cell_x = cell_id % size_x;
			const int32_t cell_y = cell_id / size_x;

			const Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x + cell_x, offset_y + cell_y);
			const Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(gt, offset_x + cell_x + 1, offset_y + cell_y + 1);
			const Point2D ptc = Point2D(pt0.x + 0.5 * (pt2.x - pt0.x), pt0.y + 0.5 * (pt2.y - pt0.y));
			const std::string geometry_wkt = StringUtil::Format("POINT (%f %f)", ptc.x, ptc.y);

			const CellRow cell_row {Value::BIGINT(row_id + i),
			                        Value::DOUBLE(ptc.x),
			                        Value::DOUBLE(ptc.y),
			                        Value(geometry_wkt),
			                        Value::INTEGER(offset_x + cell_x),
			                        Value::INTEGER(offset_y + cell_y)};

			result.processed++;

			// The filter expressions were evaluated but raster tile does not match the conditions?
			if (!FilterEval::Eval(cell_row, filter_context)) {
				RASTER_SCAN_DEBUG_LOG(3, " > cell_id=(%ld): tile did not match filter conditions, skipped", row_id + i);
				continue;
			}

			// Fill the output chunk for the current row.
			for (idx_t col_idx = 0; col_idx < column_ids.size(); col_idx++) {
				const auto &dim_index = column_ids[col_idx];

				switch (dim_index) {
				case CELL_ROWID_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.row_id);
					break;
				case CELL_X_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.x);
					break;
				case CELL_Y_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.y);
					break;
				case CELL_GEOMETRY_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.geometry);
					break;
				case CELL_COL_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.col);
					break;
				case CELL_ROW_COLUMN_INDEX:
					output.data[col_idx].SetValue(row_idx, cell_row.row);
					break;
				default: {
					const int32_t band_index = static_cast<int32_t>(dim_index - CELL_FIRST_BAND_COLUMN_INDEX);
					const size_t band_offset =
					    (band_index * size_x * size_y * data_size) + ((cell_y * size_x + cell_x) * data_size);

					// Read the band data?
					if (!data_read) {
						const size_t cube_size = static_cast<size_t>(num_bands) * size_x * size_y * data_size;
						data_buffer.GrowCapacity(cube_size);

						// Read the data of all bands...
						CPLErr read_err =
						    dataset->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_buffer.GetData(),
						                      size_x, size_y, data_type, num_bands, nullptr, 0, 0, 0, nullptr);

						// Is there an error reading the tile data?
						if (read_err != CE_None) {
							const std::string error = RasterUtils::GetLastGdalErrorMsg();
							throw IOException("Failed to read tile (%d, %d): %s", tile_x, tile_y, error.c_str());
						}
						data_read = true;
					}

					// Write band pixel value into the column.
					switch (data_type) {
					case GDT_Byte: {
						uint8_t value = *reinterpret_cast<uint8_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::UTINYINT(value));
						break;
					}
					case GDT_Int8: {
						int8_t value = *reinterpret_cast<int8_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::TINYINT(value));
						break;
					}
					case GDT_UInt16: {
						uint16_t value = *reinterpret_cast<uint16_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::USMALLINT(value));
						break;
					}
					case GDT_Int16: {
						int16_t value = *reinterpret_cast<int16_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::SMALLINT(value));
						break;
					}
					case GDT_UInt32: {
						uint32_t value = *reinterpret_cast<uint32_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::UINTEGER(value));
						break;
					}
					case GDT_Int32: {
						int32_t value = *reinterpret_cast<int32_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::INTEGER(value));
						break;
					}
					case GDT_UInt64: {
						uint64_t value = *reinterpret_cast<uint64_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::UBIGINT(value));
						break;
					}
					case GDT_Int64: {
						int64_t value = *reinterpret_cast<int64_t *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::BIGINT(value));
						break;
					}
					case GDT_Float32: {
						float value = *reinterpret_cast<float *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::FLOAT(value));
						break;
					}
					case GDT_Float64: {
						double value = *reinterpret_cast<double *>(data_buffer.GetData() + band_offset);
						output.data[col_idx].SetValue(row_idx, Value::DOUBLE(value));
						break;
					}
					default:
						throw IOException("Unsupported data type: %d", data_type);
					}
				}
				}
			}
			result.accepted++;
			row_idx++;

			if (result.accepted >= static_cast<int64_t>(max_output)) {
				break;
			}
		}
		return result;
	}

	static void Execute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
		auto &bind_data = input.bind_data->Cast<RT_Read::BindData>();
		auto &gstate = input.global_state->Cast<RT_Read::State>();

		// If we've already read all the rows/cells, return an empty chunk to indicate we're done.
		const idx_t start_row = bind_data.row_offset + gstate.current_row;
		if (start_row >= bind_data.row_count) {
			output.SetCardinality(0);
			return;
		}

		// Calculate how many record we can fit in the output, and cell coordinates to read.

		const idx_t vector_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, bind_data.row_count - start_row);
		idx_t row_id = bind_data.row_offset + gstate.current_row;
		idx_t output_size = 0;

		const int32_t &tiles_x = bind_data.tiles_x;
		const int32_t &tiles_y = bind_data.tiles_y;
		int32_t start_ty = -1;
		int32_t start_tx = -1;
		bool found = FindTileCoordinates(bind_data, row_id, start_tx, start_ty);
		if (!found) {
			throw IOException("Failed to find tile coordinates for cell id %" PRIu64, row_id);
		}

		RASTER_SCAN_DEBUG_LOG(2, "Execute: start_row=%" PRIu64 ", vector_size=%" PRIu64, start_row, vector_size);

		const auto &filter_expressions = bind_data.filter_expressions;
		const auto &column_ids = bind_data.column_ids;
		const auto &column_types = bind_data.column_types;
		const FilterContext filter_context(context, filter_expressions, column_ids, column_types);

		// Loop over the tiles and fill the output chunk, it reuses DataCubes for avoiding repeated memory allocations.

		DataCube data_cube(Allocator::Get(context));

		for (int32_t tile_y = start_ty; tile_y < tiles_y && output_size < vector_size; ++tile_y) {
			const int32_t first_tx = (tile_y == start_ty) ? start_tx : 0;

			for (int32_t tile_x = first_tx; tile_x < tiles_x && output_size < vector_size; ++tile_x) {
				const int32_t max_output = vector_size - output_size;

				// Process cells in one tile.
				OutputResult result = FillOutput(bind_data, row_id, output_size, max_output, tile_x, tile_y,
				                                 filter_context, data_cube, output);

				output_size += result.accepted;
				row_id += result.processed;
			}
		}

		// Update the row index.
		gstate.current_row = (row_id - bind_data.row_offset);

		// Set the cardinality of the output.
		output.SetCardinality(output_size);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Open a raster file (or a mosaic of raster files) and return a table with one row per value cell in the raster.

		The `RT_ReadCells` table function is based on the [GDAL](https://gdal.org/index.html) translator library and enables reading raster data from a variety of geospatial raster file formats as if they were DuckDB tables.

		> See [RT_Drivers](#rt_drivers) for a list of supported file formats and drivers.

		The `RT_ReadCells` function accepts parameters, most of them optional:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `path` | VARCHAR | The path to the file to read. The only mandatory parameter. |
		| `open_options` | VARCHAR[] | A list of key-value pairs that are passed to the GDAL driver to control the opening of the file. Refer to the GDAL documentation for available options. Only for single-file version of the function. |
		| `allowed_drivers` | VARCHAR[] | A list of GDAL driver names that are allowed to be used to open the file. If empty, all drivers are allowed. Only for single-file version of the function. |
		| `sibling_files` | VARCHAR[] | A list of sibling files that are required to open the file. Only for single-file version of the function. |
		| `separate_bands` | BOOLEAN | `true` means that each input goes into a separate band in the VRT dataset. Otherwise, the files are considered as source rasters of a larger mosaic and the VRT file has the same number of bands as the input files. Only for multi-file version of the function. `false` is the default. |

		This is the list of columns returned by `RT_ReadCells`:

		+ `id` is a unique identifier for each cell of the raster.
		+ `x` the coordinates of the center of each cell in the raster. The coordinate reference system is the same as the one of the raster file.
		+ `y` the coordinates of the center of each cell in the raster. The coordinate reference system is the same as the one of the raster file.
		+ `geometry` is the point geometry of each cell.
		+ `col` is the column index of each cell in the raster, where 0 is the leftmost column.
		+ `row` is the row index of each cell in the raster, where 0 is the topmost row.
		+ `band_1`, `band_2`, … are numeric columns, each containing the pixel values of a cell in the raster. The number of bands depends on the input file, and the data type depends on the raster data type.

		Note that GDAL is single-threaded, so this table function cannot fully exploit DuckDB parallelism.
	)";

	static constexpr auto EXAMPLE = R"(
		-- Read a Gtiff file as points.
		SELECT id, x, y, geometry, col, row, band_1 FROM RT_ReadCells('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "table");

		// Configure the functions and register them in the function set.

		TableFunctionSet function_set("RT_ReadCells");

		TableFunction func_01("RT_ReadCells", {LogicalType::VARCHAR}, Execute, BindUniqueFile, RT_Read::Init);
		func_01.named_parameters["open_options"] = LogicalType::LIST(LogicalType::VARCHAR);
		func_01.named_parameters["allowed_drivers"] = LogicalType::LIST(LogicalType::VARCHAR);
		func_01.named_parameters["sibling_files"] = LogicalType::LIST(LogicalType::VARCHAR);

		TableFunction func_02("RT_ReadCells", {LogicalType::LIST(LogicalType::VARCHAR)}, Execute, BindMultiFile,
		                      RT_Read::Init);
		func_02.named_parameters["separate_bands"] = LogicalType::BOOLEAN;

		for (auto *func : {&func_01, &func_02}) {
			func->cardinality = RT_Read::Cardinality;
			func->table_scan_progress = RT_Read::Progress;

			// Enable projection pushdown - allows DuckDB to tell us which columns are needed
			// The column_ids will be passed to InitGlobal via TableFunctionInitInput
			func->projection_pushdown = true;

			// Enable complex filter pushdown - handles expressions like (A AND B) OR (C AND D)
			// that cannot be represented as simple TableFilter objects
			func->pushdown_complex_filter = RT_Read::PushdownComplexFilter;

			function_set.AddFunction(*func);
		}

		RegisterFunction<TableFunctionSet>(loader, function_set, CatalogType::TABLE_FUNCTION_ENTRY, DESCRIPTION,
		                                   EXAMPLE, tags);

		auto &db = loader.GetDatabaseInstance();
		auto &config = DBConfig::GetConfig(db);

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
	RT_ReadCells::Register(loader);
}

} // namespace duckdb
