#pragma once

namespace duckdb {

class ExtensionLoader;

struct ProjModule {
public:
	static void Register(ExtensionLoader &loader);
};

} // namespace duckdb
