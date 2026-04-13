#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterWriteFunction {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
