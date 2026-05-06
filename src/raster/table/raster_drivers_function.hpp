#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterDriversFunction {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
