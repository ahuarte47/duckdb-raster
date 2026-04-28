#include "raster_types.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

LogicalType RasterTypes::BBOX() {
	auto bbox_type = LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
	                                      {"ymin", LogicalType::DOUBLE},
	                                      {"xmax", LogicalType::DOUBLE},
	                                      {"ymax", LogicalType::DOUBLE}});

	bbox_type.SetAlias("RT_BBOX");
	return bbox_type;
}

LogicalType RasterTypes::DATACUBE() {
	auto cube_type = LogicalType(LogicalTypeId::BLOB);
	cube_type.SetAlias("RT_DATACUBE");
	return cube_type;
}

LogicalType RasterTypes::ARRAY(const LogicalType &element_type) {
	auto array_type = LogicalType::STRUCT({{"data_type", LogicalType::INTEGER},
	                                       {"bands", LogicalType::INTEGER},
	                                       {"cols", LogicalType::INTEGER},
	                                       {"rows", LogicalType::INTEGER},
	                                       {"no_data", LogicalType::DOUBLE},
	                                       {"values", LogicalType::LIST(element_type)}});
	array_type.SetAlias("RT_ARRAY");
	return array_type;
}

LogicalType RasterTypes::STATS() {
	auto stats_type = LogicalType::STRUCT({{"minimum", LogicalType::DOUBLE},
	                                       {"maximum", LogicalType::DOUBLE},
	                                       {"mean", LogicalType::DOUBLE},
	                                       {"stddev", LogicalType::DOUBLE},
	                                       {"valid_count", LogicalType::BIGINT},
	                                       {"nodata_count", LogicalType::BIGINT}});
	stats_type.SetAlias("RT_STATS");
	return stats_type;
}

void RasterTypes::Register(ExtensionLoader &loader) {
	// Register types
	loader.RegisterType("RT_BBOX", RasterTypes::BBOX());
	loader.RegisterType("RT_DATACUBE", RasterTypes::DATACUBE());
	loader.RegisterType("RT_STATS", RasterTypes::STATS());
}

} // namespace duckdb
