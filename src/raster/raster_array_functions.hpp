#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterArrayFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
