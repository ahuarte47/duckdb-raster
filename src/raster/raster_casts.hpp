#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterCastsFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
