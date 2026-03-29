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

void RasterTypes::Register(ExtensionLoader &loader) {
	// Register types
	loader.RegisterType("RT_BBOX", RasterTypes::BBOX());
}

} // namespace duckdb
