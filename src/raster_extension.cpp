#define DUCKDB_EXTENSION_MAIN

#include "raster_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// GDAL/Raster
#include "raster/gdal_module.hpp"
#include "raster/raster_types.hpp"
#include "raster/raster_drivers_function.hpp"
#include "raster/raster_read_function.hpp"
#include "raster/raster_array_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the GDAL module for RASTER
	GdalModule::Register(loader);
	// Register RASTER types and functions
	RasterTypes::Register(loader);
	RasterDriversFunction::Register(loader);
	RasterReadFunction::Register(loader);
	RasterArrayFunctions::Register(loader);
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
