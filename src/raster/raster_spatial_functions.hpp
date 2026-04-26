#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterSpatialFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
