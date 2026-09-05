#include "gdal_dataset_io.hpp"
#include "gdal_file_system.hpp"
// Just for RASTER_SCAN_DEBUG_LOG macro
#include "../raster_utils.hpp"

// DuckDB
#include "duckdb/common/file_system.hpp"
// GDAL
#include "gdal_utils.h"

namespace duckdb {

//======================================================================================================================
// Utilities
//======================================================================================================================

//! Resolve a file path into a GDAL-accessible path.
std::string DuckDBDatasetFactory::GdalFilePath(ClientContext &context, const std::string &file_path) {
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
		const duckdb::vector<duckdb::Value> params = ListValue::GetChildren(input_param->second);
		output.reserve(params.size() + 1);

		for (auto &param : params) {
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
		const duckdb::vector<duckdb::Value> params = ListValue::GetChildren(input_param->second);
		output.reserve(params.size() + 1);

		for (auto &param : params) {
			const auto file_path = StringValue::Get(param);
			output.push_back(DuckDBDatasetFactory::GdalFilePath(context, file_path).c_str());
		}
		output.push_back(nullptr);
	}
	return output;
}

//======================================================================================================================
// Dataset IO
//======================================================================================================================

GDALDataset *DuckDBDatasetFactory::OpenDataset(ClientContext &context, const std::vector<std::string> &file_names,
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

GDALDataset *DuckDBDatasetFactory::WarpDataset(ClientContext &context, const std::vector<std::string> &file_names,
                                               const named_parameter_map_t &params,
                                               std::vector<GDALDatasetUniquePtr> &child_datasets) {
	GDALDataset *result = nullptr;

	if (file_names.size() == 1) {
		GDALDatasetUniquePtr dataset = GDALDatasetUniquePtr(OpenDataset(context, file_names, params));

		auto input_param = params.find("warp_options");
		if (input_param == params.end() || input_param->second.type().id() != LogicalTypeId::LIST) {
			return dataset.release();
		}

		const auto options = ListValue::GetChildren(input_param->second);
		if (options.empty()) {
			return dataset.release();
		}

		// Load warp options from the named parameters,
		// use a named /vsimem GeoTIFF output so downstream VRT sources are stable and reopenable.

		char **papszArgv = nullptr;
		papszArgv = CSLAddString(papszArgv, "-of");
		papszArgv = CSLAddString(papszArgv, "GTiff");

		for (auto it = options.begin(); it != options.end(); ++it) {
			const auto option_str = StringValue::Get(*it);
			papszArgv = CSLAddString(papszArgv, option_str.c_str());
		}

		// Warp the dataset using GDAL's in-memory filesystem.

		GDALWarpAppOptions *psOptions = GDALWarpAppOptionsNew(papszArgv, nullptr);
		CSLDestroy(papszArgv);
		CPLErrorReset();

		const std::string ds_name = "/vsimem/" + UUID::ToString(UUID::GenerateRandomUUID()) + ".tif";
		GDALDatasetH ds_handle = GDALDataset::ToHandle(dataset.get());
		result = GDALDataset::FromHandle(GDALWarp(ds_name.c_str(), nullptr, 1, &ds_handle, psOptions, nullptr));
		GDALWarpAppOptionsFree(psOptions);

		if (result) {
			result->FlushCache();
		} else {
			const std::string error = RasterUtils::GetLastGdalErrorMsg();
			throw IOException("Failed to warp dataset (" + error + ")");
		}
	} else {
		bool separate_bands = false;

		if (params.find("separate_bands") != params.end()) {
			separate_bands = params.at("separate_bands").GetValue<bool>();
		}

		std::vector<GDALDatasetUniquePtr> temp_datasets;

		for (const auto &file_name : file_names) {
			GDALDatasetUniquePtr child(WarpDataset(context, {file_name}, params, child_datasets));
			temp_datasets.push_back(std::move(child));
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

		std::vector<GDALDatasetH> ds_handles;
		for (auto &ds : temp_datasets) {
			ds_handles.push_back(GDALDataset::ToHandle(ds.get()));
			child_datasets.push_back(std::move(ds));
		}

		result = GDALDataset::FromHandle(GDALBuildVRT(vrt_path.c_str(), static_cast<int>(ds_handles.size()),
		                                              ds_handles.data(), nullptr, vrt_opts.get(), nullptr));

		if (!result) {
			const std::string error = RasterUtils::GetLastGdalErrorMsg();
			throw IOException("Failed to build VRT mosaic from input files (" + error + ")");
		}
		RASTER_SCAN_DEBUG_LOG(1, "GDAL dataset opened: '%s'", "<multiple files>");
	}
	return result;
}

GDALDataset *DuckDBDatasetFactory::CreateDataset(ClientContext &context, const std::string &file_name,
                                                 const named_parameter_map_t &params) {
	auto input_param = params.find("create_options");
	if (input_param == params.end() || input_param->second.type().id() != LogicalTypeId::LIST) {
		throw InvalidInputException("Missing or invalid 'create_options' parameter for dataset creation");
	}

	auto create_options = ListValue::GetChildren(input_param->second);
	if (create_options.size() != 10) {
		int32_t num_options = static_cast<int32_t>(create_options.size());
		throw InvalidInputException("Expected 10 parameters for dataset creation, got " + std::to_string(num_options));
	}

	// Fetch the parameters for creating a new raster dataset.

	std::string crs = StringValue::Get(create_options[0]);
	double x_min = create_options[1].GetValue<double>();
	double y_min = create_options[2].GetValue<double>();
	double x_max = create_options[3].GetValue<double>();
	double y_max = create_options[4].GetValue<double>();
	int32_t x_size = create_options[5].GetValue<int32_t>();
	int32_t y_size = create_options[6].GetValue<int32_t>();
	int32_t n_bands = create_options[7].GetValue<int32_t>();
	int32_t data_type = create_options[8].GetValue<int32_t>();
	double nodata_value = create_options[9].GetValue<double>();

	if (crs.empty()) {
		throw InvalidInputException("CRS string cannot be empty for dataset creation");
	}
	if (n_bands <= 0) {
		throw InvalidInputException("Number of bands must be positive for dataset creation");
	}
	if (x_size <= 0 || y_size <= 0) {
		throw InvalidInputException("Raster size must be positive for dataset creation");
	}
	if (data_type <= GDT_Unknown || data_type > GDT_Float64) {
		throw InvalidInputException("Invalid data type for dataset creation");
	}

	// Get the driver for creating the raster file.

	std::string driver_name = "Memory";

	if (params.find("driver_name") != params.end()) {
		driver_name = StringValue::Get(params.at("driver_name"));
	}

	auto driver = GetGDALDriverManager()->GetDriverByName(driver_name.c_str());
	if (!driver) {
		throw InvalidInputException("'" + driver_name + "' driver not found");
	}

	// Cofigure options for the driver, if any.

	int32_t blocksize_x = 0;

	if (params.find("blocksize_x") != params.end()) {
		blocksize_x = params.at("blocksize_x").GetValue<int32_t>();

		if (blocksize_x <= 0) {
			throw InvalidInputException("Block size must be positive for dataset creation");
		}
	}

	int32_t blocksize_y = 0;

	if (params.find("blocksize_y") != params.end()) {
		blocksize_y = params.at("blocksize_y").GetValue<int32_t>();

		if (blocksize_y <= 0) {
			throw InvalidInputException("Block size must be positive for dataset creation");
		}
	}

	// Create the dataset with the specified parameters.

	RASTER_SCAN_DEBUG_LOG(2, "Creating file '%s': data_type=%d, bands=%d, size=(%d x %d), extent=(%lf, %lf, %lf, %lf)",
	                      file_name.c_str(), data_type, n_bands, x_size, y_size, x_min, y_min, x_max, y_max);

	char **driver_options = nullptr;

	if (blocksize_x > 0 && blocksize_y > 0) {
		driver_options = CSLAddString(driver_options, ("BLOCKXSIZE=" + std::to_string(blocksize_x)).c_str());
		driver_options = CSLAddString(driver_options, ("BLOCKYSIZE=" + std::to_string(blocksize_y)).c_str());
	}

	GDALDataType gdal_type = static_cast<GDALDataType>(data_type);
	GDALDatasetUniquePtr dataset(driver->Create(file_name.c_str(), x_size, y_size, n_bands, gdal_type, driver_options));
	dataset->SetProjection(crs.c_str());
	double gt[6] = {x_min, (x_max - x_min) / x_size, 0, y_max, 0, (y_min - y_max) / y_size};
	dataset->SetGeoTransform(gt);

	CSLDestroy(driver_options);

	for (int32_t b = 1; b <= n_bands; b++) {
		GDALRasterBand *band = dataset->GetRasterBand(b);
		if (!band) {
			throw InternalException("Failed to get raster band from dataset");
		}
		band->SetNoDataValue(nodata_value);
		band->Fill(nodata_value);
	}
	dataset->FlushCache();

	return dataset.release();
}

} // namespace duckdb
