#pragma once

namespace duckdb {

class ExtensionLoader;

struct RasterMathFunctions {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
