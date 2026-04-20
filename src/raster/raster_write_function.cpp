#include "raster_write_function.hpp"
#include "raster_types.hpp"
#include "raster_utils.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/storage/buffer_manager.hpp"

// GDAL
#include "gdal_priv.h"
#include "gdal_utils.h"

// STL
#include <string>
#include <cstdlib>
#if defined(_WIN32) || defined(WIN32)
#include <windows.h>
#endif

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
// RT_Write
//======================================================================================================================

struct RT_Write {
	//------------------------------------------------------------------------------------------------------------------
	// Bind
	//------------------------------------------------------------------------------------------------------------------

	struct BindData : public TableFunctionData {
		std::string file_path;

		std::string driver_name = "GTiff";
		std::vector<std::string> creation_options;
		std::string resampling_alg = "nearest";

		double output_envelope[4] = {0, 0, 0, 0};
		std::string output_srs;

		int geometry_col = -1;
		std::vector<int> databand_cols;

		BindData(std::string file_path) : file_path(std::move(file_path)) {
		}
	};

	static unique_ptr<FunctionData> Bind(ClientContext &context, CopyFunctionBindInput &input,
	                                     const vector<string> &names, const vector<LogicalType> &types) {
		auto file_path = input.info.file_path;
		auto bind_data = make_uniq<BindData>(file_path);

		// Check the options in the copy info and set the bind data accordingly

		std::string geometry_col_name = "geometry";
		std::vector<std::string> databand_col_names;

		for (auto &option : input.info.options) {
			std::string key = StringUtil::Upper(option.first);

			if (key == "DRIVER") {
				auto set = option.second.front();
				if (set.type().id() == LogicalTypeId::VARCHAR) {
					bind_data->driver_name = set.GetValue<string>();
				} else {
					throw BinderException("Driver name must be a string.");
				}
			} else if (key == "CREATION_OPTIONS") {
				auto set = option.second;
				for (auto &s : set) {
					if (s.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("Creation options must be strings.");
					}
					bind_data->creation_options.push_back(s.GetValue<string>());
				}
			} else if (key == "ENVELOPE") {
				auto set = option.second;
				if (set.size() == 1 && set[0].type().id() == LogicalTypeId::LIST) {
					set = ListValue::GetChildren(set[0]);
				}
				if (set.size() != 4) {
					throw BinderException("Output envelope must have 4 values: minX, minY, maxX, maxY.");
				}
				for (idx_t i = 0; i < 4; i++) {
					bind_data->output_envelope[i] = set[i].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
				}
			} else if (key == "SRS") {
				auto set = option.second.front();
				if (set.type().id() == LogicalTypeId::VARCHAR) {
					bind_data->output_srs = set.GetValue<string>();
				} else {
					throw BinderException("Output SRS must be a string.");
				}

			} else if (key == "RESAMPLING") {
				auto set = option.second.front();
				if (set.type().id() == LogicalTypeId::VARCHAR) {
					bind_data->resampling_alg = set.GetValue<string>();
				} else {
					throw BinderException("Resampling algorithm must be a string.");
				}
			} else if (key == "GEOMETRY_COLUMN") {
				auto set = option.second.front();
				if (set.type().id() == LogicalTypeId::VARCHAR) {
					geometry_col_name = set.GetValue<string>();
				} else {
					throw BinderException("Geometry column name must be a string.");
				}
			} else if (key == "DATABAND_COLUMNS") {
				auto set = option.second;
				if (set.size() == 1 && set[0].type().id() == LogicalTypeId::LIST) {
					set = ListValue::GetChildren(set[0]);
				}
				for (auto &s : set) {
					if (s.type().id() != LogicalTypeId::VARCHAR) {
						throw BinderException("Databand column names must be strings.");
					}
					databand_col_names.push_back(s.GetValue<string>());
				}
			} else {
				throw BinderException("Unknown option '%s'.", option.first);
			}
		}

		auto driver = GetGDALDriverManager()->GetDriverByName(bind_data->driver_name.c_str());
		if (!driver) {
			throw BinderException("Unknown driver '%s'.", bind_data->driver_name);
		}

		// Get indices of geometry and band columns (The unique mandatory in the input)

		for (idx_t i = 0; i < names.size(); i++) {
			const auto &col_name = names[i];
			const auto &col_type = types[i];

			if (col_name == geometry_col_name) {
				if (col_type.id() != LogicalTypeId::GEOMETRY) {
					throw BinderException("Geometry column must be of type GEOMETRY.");
				}
				bind_data->geometry_col = i;
				break;
			}
		}
		for (const auto &databand_col_name : databand_col_names) {
			for (idx_t i = 0; i < names.size(); i++) {
				const auto &col_name = names[i];
				const auto &col_type = types[i];

				if (col_name == databand_col_name) {
					if (col_type.id() != LogicalTypeId::BLOB) {
						throw BinderException("Databand columns must be of type BLOB.");
					}
					bind_data->databand_cols.push_back(i);
					break;
				}
			}
		}

		if (bind_data->geometry_col == -1) {
			throw BinderException("No geometry column configured in input.");
		}
		if (bind_data->databand_cols.empty()) {
			throw BinderException("No databand columns configured in input.");
		}

		// Try get the file extension from the driver

		auto file_ext = driver->GetMetadataItem(GDAL_DMD_EXTENSION);
		if (file_ext) {
			input.file_extension = file_ext;
		} else {
			auto file_exts = driver->GetMetadataItem(GDAL_DMD_EXTENSIONS);
			if (file_exts) {
				auto exts = StringUtil::Split(file_exts, ' ');
				if (!exts.empty()) {
					input.file_extension = exts[0];
				}
			}
		}

		return std::move(bind_data);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init Global
	//------------------------------------------------------------------------------------------------------------------

	struct GlobalState final : GlobalFunctionData {
		std::vector<GDALDatasetUniquePtr> input_tiles;

		explicit GlobalState(ClientContext &context) {
		}
	};

	static unique_ptr<GlobalFunctionData> InitGlobal(ClientContext &context, FunctionData &bind_data,
	                                                 const string &file_path) {
		auto global_data = make_uniq<GlobalState>(context);
		return std::move(global_data);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Init Local
	//------------------------------------------------------------------------------------------------------------------

	struct LocalState : public LocalFunctionData {
		explicit LocalState(ClientContext &context) {
		}
	};

	static unique_ptr<LocalFunctionData> InitLocal(ExecutionContext &context, FunctionData &bind_data) {
		auto local_data = make_uniq<LocalState>(context.client);
		return std::move(local_data);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Sink
	//------------------------------------------------------------------------------------------------------------------

	static void Sink(ExecutionContext &context, FunctionData &fdata, GlobalFunctionData &gstate,
	                 LocalFunctionData &lstate, DataChunk &input) {
		auto &bind_data = fdata.Cast<BindData>();
		auto &global_state = gstate.Cast<GlobalState>();

		// Get the driver for creating the in-memory tiles

		auto driver = GetGDALDriverManager()->GetDriverByName("MEM");
		if (!driver) {
			throw InternalException("MEM driver not found");
		}

		// Accumulate input tiles (rows) into temporary rasters

		std::vector<DataCube> data_cubes;

		input.data[bind_data.geometry_col].Flatten(input.size());
		const string_t *geom_data = FlatVector::GetData<string_t>(input.data[bind_data.geometry_col]);

		for (idx_t band_idx : bind_data.databand_cols) {
			input.data[band_idx].Flatten(input.size());
			data_cubes.emplace_back(Allocator::Get(context.client));
		}

		for (idx_t row_idx = 0; row_idx < input.size(); row_idx++) {
			GeometryExtent extent = GeometryExtent::Empty();
			const auto g_is_empty = Geometry::GetExtent(geom_data[row_idx], extent) == 0;
			if (g_is_empty) {
				// If we can't get the extent, we skip writing this tile
				continue;
			}

			const auto &x_min = extent.x_min;
			const auto &y_min = extent.y_min;
			const auto &x_max = extent.x_max;
			const auto &y_max = extent.y_max;

			// Extract the tile data from the band columns

			GDALDataType data_type = GDT_Unknown;
			int cube_idx = 0;
			int n_bands = 0;
			int x_size = 0;
			int y_size = 0;

			for (idx_t band_idx : bind_data.databand_cols) {
				const auto &band_value = input.data[band_idx].GetValue(row_idx);

				DataCube &data_cube = data_cubes[cube_idx++];

				if (data_cube.LoadBlob(band_value) > 0) {
					DataHeader tile_header = data_cube.GetHeader();
					// Process the data band
					GDALDataType data_type_i = RasterUtils::DataTypeToGdalType(tile_header.data_type);
					int x_size_i = tile_header.cols;
					int y_size_i = tile_header.rows;

					if (data_type == GDT_Unknown) {
						data_type = data_type_i;
					} else if (data_type != data_type_i) {
						global_state.input_tiles.clear();
						throw std::runtime_error("Inconsistent data types in input data bands");
					}
					if (x_size == 0 && y_size == 0) {
						x_size = x_size_i;
						y_size = y_size_i;
					} else if (x_size != x_size_i || y_size != y_size_i) {
						global_state.input_tiles.clear();
						throw std::runtime_error("Inconsistent tile sizes in input data bands");
					}
					n_bands += tile_header.bands;

				} else {
					// Empty data band
				}
			}

			if (n_bands == 0 || x_size == 0 || y_size == 0) {
				RASTER_SCAN_DEBUG_LOG(2, "Skipping empty tile at row %lu", row_idx);
				continue;
			}

			// Create the dataset for the tile and write the data to it

			RASTER_SCAN_DEBUG_LOG(
			    2, "Writing tile for row %lu: data_type=%d, bands=%d, size=(%d x %d), extent=(%lf, %lf, %lf, %lf)",
			    row_idx, data_type, n_bands, x_size, y_size, x_min, y_min, x_max, y_max);

			GDALDatasetUniquePtr dataset(driver->Create("", x_size, y_size, n_bands, data_type, nullptr));

			if (!bind_data.output_srs.empty()) {
				dataset->SetProjection(bind_data.output_srs.c_str());
			}

			double geo_transform[6] = {x_min, (x_max - x_min) / x_size, 0, y_max, 0, (y_min - y_max) / y_size};
			dataset->SetGeoTransform(geo_transform);

			int data_size = GDALGetDataTypeSizeBytes(data_type);
			int b = 1;

			for (DataCube &data_cube : data_cubes) {
				data_ptr_t data_ptr = data_cube.GetBuffer().GetData() + sizeof(DataHeader);
				DataHeader header = data_cube.GetHeader();

				for (int i = 0; i < header.bands; i++) {
					GDALRasterBand *band = dataset->GetRasterBand(b);
					if (!band) {
						throw std::runtime_error("Failed to get raster band from dataset");
					}
					band->SetNoDataValue(header.no_data);

					if (band->RasterIO(GF_Write, 0, 0, x_size, y_size, data_ptr, x_size, y_size, data_type, 0, 0) !=
					    CE_None) {
						throw std::runtime_error("Failed to write tile data to dataset");
					}
					data_ptr += (x_size * y_size * data_size);
					b++;
				}
			}

			// Keep the dataset for later processing in Finalize
			global_state.input_tiles.push_back(std::move(dataset));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Combine
	//------------------------------------------------------------------------------------------------------------------

	static void Combine(ExecutionContext &context, FunctionData &fdata, GlobalFunctionData &gstate,
	                    LocalFunctionData &lstate) {
	}

	//------------------------------------------------------------------------------------------------------------------
	// Finalize
	//------------------------------------------------------------------------------------------------------------------

	//! Get the temporary directory for creating intermediate files.
	static std::string GetTemporaryDirectory() {
#if defined(_WIN32) || defined(WIN32)
		static constexpr DWORD MAX_PATH_LENGTH = MAX_PATH + 1;
		char tmpdir[MAX_PATH_LENGTH];
		DWORD ret = GetTempPathA(MAX_PATH_LENGTH, tmpdir);
		if (ret == 0 || ret > MAX_PATH_LENGTH) {
			throw std::runtime_error("Cannot locate temporary directory");
		}
		return tmpdir;
#else
		const char *envs[] = {"TMPDIR", "TMP", "TEMP", nullptr};
		for (int i = 0; envs[i]; ++i) {
			const char *val = std::getenv(envs[i]);
			if (val && val[0] != '\0')
				return std::string(val);
		}
		return "/tmp";
#endif
	}

	static void Finalize(ClientContext &context, FunctionData &fdata, GlobalFunctionData &gstate) {
		auto &bind_data = fdata.Cast<BindData>();
		auto &global_state = gstate.Cast<GlobalState>();

		std::string file_path = bind_data.file_path;

		if (global_state.input_tiles.empty()) {
			RASTER_SCAN_DEBUG_LOG(1, "No tiles were written, skipping creation of raster '%s'", file_path.c_str());
			return;
		}

		// Create a Virtual Raster (VRT) from the in-memory tiles created in the sink phase,
		// then create the final raster using the VRT as source.

		const std::string temp_dir = RT_Write::GetTemporaryDirectory();
		auto &fs = FileSystem::GetFileSystem(context);

		// Build VRT mosaic from in-memory input tiles

		std::vector<GDALDatasetH> dataset_handles;
		dataset_handles.reserve(global_state.input_tiles.size());
		for (auto &tile : global_state.input_tiles) {
			dataset_handles.push_back(static_cast<GDALDatasetH>(tile.get()));
		}

		std::string vrt_path = fs.JoinPath(temp_dir, UUID::ToString(UUID::GenerateRandomUUID()) + ".vrt");
		const char *vrt_argv[] = {"-r", bind_data.resampling_alg.c_str(), nullptr};

		using GDALBuildVRTOptionsPtr = std::unique_ptr<GDALBuildVRTOptions, decltype(&GDALBuildVRTOptionsFree)>;

		GDALBuildVRTOptionsPtr vrt_opts(GDALBuildVRTOptionsNew(const_cast<char **>(vrt_argv), nullptr),
		                                GDALBuildVRTOptionsFree);

		GDALDatasetUniquePtr vrt_dataset(
		    GDALDataset::FromHandle(GDALBuildVRT(vrt_path.c_str(), static_cast<int>(dataset_handles.size()),
		                                         dataset_handles.data(), nullptr, vrt_opts.get(), nullptr)));

		vrt_opts.reset();

		if (!vrt_dataset) {
			global_state.input_tiles.clear();
			throw std::runtime_error("Failed to build VRT mosaic from input tiles");
		}

		// Build GDALTranslate arguments: driver, SRS, envelope, and creation options

		std::vector<std::string> translate_args;

		translate_args.push_back("-of");
		translate_args.push_back(bind_data.driver_name);

		if (!bind_data.output_srs.empty()) {
			translate_args.push_back("-a_srs");
			translate_args.push_back(bind_data.output_srs);
		}

		const bool has_envelope = (bind_data.output_envelope[2] != bind_data.output_envelope[0] &&
		                           bind_data.output_envelope[3] != bind_data.output_envelope[1]);

		if (has_envelope) {
			// GDALTranslate -projwin expects: ulx uly lrx lry -> xmin ymax xmax ymin
			translate_args.push_back("-projwin");
			translate_args.push_back(std::to_string(bind_data.output_envelope[0])); // xmin
			translate_args.push_back(std::to_string(bind_data.output_envelope[3])); // ymax
			translate_args.push_back(std::to_string(bind_data.output_envelope[2])); // xmax
			translate_args.push_back(std::to_string(bind_data.output_envelope[1])); // ymin
		}

		for (const auto &opt : bind_data.creation_options) {
			translate_args.push_back("-co");
			translate_args.push_back(opt);
		}

		std::vector<const char *> translate_argv;
		translate_argv.reserve(translate_args.size() + 1);
		for (const auto &arg : translate_args) {
			translate_argv.push_back(arg.c_str());
		}
		translate_argv.push_back(nullptr);

		using GDALTranslateOptionsPtr = std::unique_ptr<GDALTranslateOptions, decltype(&GDALTranslateOptionsFree)>;

		GDALTranslateOptionsPtr translate_opts(
		    GDALTranslateOptionsNew(const_cast<char **>(translate_argv.data()), nullptr), GDALTranslateOptionsFree);

		// Create the final raster from the VRT

		RASTER_SCAN_DEBUG_LOG(1, "Writing final raster to '%s'", file_path.c_str());

		int usage_error = FALSE;
		GDALDatasetUniquePtr output(GDALDataset::FromHandle(GDALTranslate(
		    file_path.c_str(), GDALDataset::ToHandle(vrt_dataset.get()), translate_opts.get(), &usage_error)));

		if (output) {
			output->FlushCache();
		}

		// Clean up resources

		translate_opts.reset();
		vrt_dataset.reset();
		global_state.input_tiles.clear();
		fs.RemoveFile(vrt_path);

		if (!output || usage_error) {
			throw std::runtime_error("Failed to write output raster file: " + file_path);
		}
		output.reset();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		// register the copy function
		CopyFunction info("RASTER");
		info.copy_to_bind = Bind;
		info.copy_to_initialize_local = InitLocal;
		info.copy_to_initialize_global = InitGlobal;
		info.copy_to_sink = Sink;
		info.copy_to_combine = Combine;
		info.copy_to_finalize = Finalize;
		info.extension = "raster";

		loader.RegisterFunction(info);
	}
};

} // namespace

// #####################################################################################################################
// Register Write Function
// #####################################################################################################################

void RasterWriteFunction::Register(ExtensionLoader &loader) {
	RT_Write::Register(loader);
}

} // namespace duckdb
