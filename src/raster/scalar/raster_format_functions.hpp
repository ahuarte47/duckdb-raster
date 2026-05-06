#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterFormatFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
