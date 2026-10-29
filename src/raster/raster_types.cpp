#include "raster_types.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

LogicalType RasterTypes::BBOX() {
	return LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
	                            {"ymin", LogicalType::DOUBLE},
	                            {"xmax", LogicalType::DOUBLE},
	                            {"ymax", LogicalType::DOUBLE}})
	    .WithAlias("RT_BBOX");
}

LogicalType RasterTypes::DATACUBE() {
	return LogicalType(LogicalTypeId::BLOB).WithAlias("RT_DATACUBE");
}

LogicalType RasterTypes::ARRAY(const LogicalType &element_type) {
	return LogicalType::STRUCT({{"data_type", LogicalType::INTEGER},
	                            {"bands", LogicalType::INTEGER},
	                            {"cols", LogicalType::INTEGER},
	                            {"rows", LogicalType::INTEGER},
	                            {"no_data", LogicalType::DOUBLE},
	                            {"values", LogicalType::LIST(element_type)}})
	    .WithAlias("RT_ARRAY");
}

LogicalType RasterTypes::STATS() {
	return LogicalType::STRUCT({{"minimum", LogicalType::DOUBLE},
	                            {"maximum", LogicalType::DOUBLE},
	                            {"sum", LogicalType::DOUBLE},
	                            {"mean", LogicalType::DOUBLE},
	                            {"stddev", LogicalType::DOUBLE},
	                            {"valid_count", LogicalType::BIGINT},
	                            {"nodata_count", LogicalType::BIGINT}})
	    .WithAlias("RT_STATS");
}

void RasterTypes::Register(ExtensionLoader &loader) {
	// Register types
	loader.RegisterType("RT_BBOX", RasterTypes::BBOX());
	loader.RegisterType("RT_DATACUBE", RasterTypes::DATACUBE());
	loader.RegisterType("RT_STATS", RasterTypes::STATS());
}

} // namespace duckdb
