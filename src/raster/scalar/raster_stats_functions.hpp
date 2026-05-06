#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterStatsFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
