#define DUCKDB_EXTENSION_MAIN

#include "raster_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// GDAL/Raster
#include "raster/modules/gdal_module.hpp"
#include "raster/raster_casts.hpp"
#include "raster/raster_types.hpp"
#include "raster/table/raster_drivers_function.hpp"
#include "raster/table/raster_read_function.hpp"
#include "raster/table/raster_write_function.hpp"
#include "raster/scalar/raster_array_functions.hpp"
#include "raster/scalar/raster_format_functions.hpp"
#include "raster/scalar/raster_math_functions.hpp"
#include "raster/scalar/raster_spatial_functions.hpp"
#include "raster/scalar/raster_stats_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the GDAL module for RASTER
	GdalModule::Register(loader);
	// Register RASTER types and functions
	RasterTypes::Register(loader);
	RasterCastsFunctions::Register(loader);
	// Register Table functions
	RasterDriversFunction::Register(loader);
	RasterReadFunction::Register(loader);
	RasterWriteFunction::Register(loader);
	// Register Scalar functions
	RasterArrayFunctions::Register(loader);
	RasterFormatFunctions::Register(loader);
	RasterMathFunctions::Register(loader);
	RasterSpatialFunctions::Register(loader);
	RasterStatsFunctions::Register(loader);
}

void RasterExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string RasterExtension::Name() {
	return "raster";
}

std::string RasterExtension::Version() const {
#ifdef EXT_VERSION_RASTER
	return EXT_VERSION_RASTER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(raster, loader) {
	duckdb::LoadInternal(loader);
}
}
