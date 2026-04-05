#include "raster_types.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

LogicalType RasterTypes::BBOX() {
	auto bbox_type = LogicalType::STRUCT({{"xmin", LogicalType::DOUBLE},
	                                      {"ymin", LogicalType::DOUBLE},
	                                      {"xmax", LogicalType::DOUBLE},
	                                      {"ymax", LogicalType::DOUBLE}});
	return bbox_type;
}

LogicalType RasterTypes::ARRAY(const LogicalType &element_type) {
	auto array_type = LogicalType::STRUCT({{"data_type", LogicalType::INTEGER},
	                                       {"bands", LogicalType::INTEGER},
	                                       {"cols", LogicalType::INTEGER},
	                                       {"rows", LogicalType::INTEGER},
	                                       {"no_data", LogicalType::DOUBLE},
	                                       {"values", LogicalType::LIST(element_type)}});
	return array_type;
}

void RasterTypes::Register(ExtensionLoader &loader) {
	// Register types
	loader.RegisterType("RT_BBOX", RasterTypes::BBOX());
}

} // namespace duckdb
