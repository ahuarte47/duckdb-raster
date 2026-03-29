#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterReadFunction {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
