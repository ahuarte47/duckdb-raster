#pragma once

namespace duckdb {

class ExtensionLoader;

struct GdalModule {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
