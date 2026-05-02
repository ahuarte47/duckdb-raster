#include "gdal_module.hpp"
#include "function_builder.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/function/scalar_function.hpp"

// GDAL
#include "cpl_string.h"
#include "cpl_vsi.h"
#include "cpl_vsi_error.h"
#include "gdal_priv.h"
#include <mutex>

namespace duckdb {

//======================================================================================================================
// RT_GdalConfig
//======================================================================================================================

struct RT_GdalConfig {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Set a GDAL configuration option (equivalent to CPLSetConfigOption).
	static void Execute(DataChunk &args, ExpressionState & /*state*/, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		for (idx_t i = 0; i < count; i++) {
			const string key = args.data[0].GetValue(i).GetValue<string>();
			const Value val = args.data[1].GetValue(i);

			if (val.IsNull()) {
				CPLSetConfigOption(key.c_str(), nullptr);
			} else {
				CPLSetConfigOption(key.c_str(), val.GetValue<string>().c_str());
			}
			result.SetValue(i, Value::BOOLEAN(true));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Sets a GDAL configuration option (equivalent to CPLSetConfigOption).

		Pass NULL as the value to unset the option.
		This is useful, for example, to allow unauthenticated access to public S3 buckets
		when using GDAL-native VSI paths:

		SELECT RT_GdalConfig('AWS_NO_SIGN_REQUEST', 'YES');
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_GdalConfig('AWS_NO_SIGN_REQUEST', 'YES');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		const ScalarFunction func("RT_GdalConfig", {LogicalType::VARCHAR, LogicalType::VARCHAR}, LogicalType::BOOLEAN,
		                          Execute);

		RegisterFunction<ScalarFunction>(loader, func, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE, tags);
	}
};

void GdalModule::Register(ExtensionLoader &loader) {
	// Load GDAL (once)
	static std::once_flag loaded;
	std::call_once(loaded, [&]() {
		// Register all embedded drivers (dont go looking for plugins)
		GDALAllRegister();

		// Set GDAL error handler
		CPLSetErrorHandler([](CPLErr e, int code, const char *raw_msg) {
			// DuckDB doesnt do warnings, so we only throw on errors
			if (e != CE_Failure && e != CE_Fatal) {
				return;
			}

			// If the error contains a /vsiduckdb-<uuid>/ prefix,
			// try to strip it off to make the errors more readable
			auto msg = string(raw_msg);
			auto path_pos = msg.find("/vsiduckdb-");
			if (path_pos != string::npos) {
				// We found a path, strip it off
				msg.erase(path_pos, 48);
			}

			switch (code) {
			case CPLE_NoWriteAccess:
				throw PermissionException("GDAL Error (%d): %s", code, msg);
			case CPLE_UserInterrupt:
				throw InterruptException();
			case CPLE_OutOfMemory:
				throw OutOfMemoryException("GDAL Error (%d): %s", code, msg);
			case CPLE_NotSupported:
				throw NotImplementedException("GDAL Error (%d): %s", code, msg);
			case CPLE_AssertionFailed:
			case CPLE_ObjectNull:
				throw InternalException("GDAL Error (%d): %s", code, msg);
			case CPLE_IllegalArg:
				throw InvalidInputException("GDAL Error (%d): %s", code, msg);
			case CPLE_AppDefined:
			case CPLE_HttpResponse:
			case CPLE_FileIO:
			case CPLE_OpenFailed:
			default:
				throw IOException("GDAL Error (%d): %s", code, msg);
			}
		});
	});

	// Register GDAL utility functions
	RT_GdalConfig::Register(loader);
}

} // namespace duckdb
