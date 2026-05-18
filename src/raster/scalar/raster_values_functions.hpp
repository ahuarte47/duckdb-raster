#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterValuesFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
