#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterTableFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
