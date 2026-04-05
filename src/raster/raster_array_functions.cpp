#include "raster_array_functions.hpp"
#include "raster_utils.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// RT_Array
//======================================================================================================================

struct RT_Array {
	//! Transform a BLOB data band into an ARRAY of values.
	static void Blob2Array(DataChunk &args, ExpressionState &state, Vector &result, const LogicalType &type) {
		D_ASSERT(args.data.size() == 2);

		const idx_t count = args.size();

		// Process each row in the input chunk and convert the BLOB to an ARRAY.
		// We loop over the rows by hand because BinaryExecutor only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			bool filter_nodata = args.data[1].GetValue(i).GetValue<bool>();

			result.SetValue(i, RasterUtils::BlobAsArray(blob, type, filter_nodata));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Transforms a BLOB data band into an ARRAY of values.

		The BLOB is expected to contain the raw data of a raster tile, along with its metadata (e.g. dimensions,
		data type, nodata value). The function reads the BLOB, extracts the metadata, and converts the raw tile data
		into an ARRAY of values that can be easily queried and manipulated in DuckDB.

		If the `filter_nodata` parameter is set to true, any values in the tile that match the nodata value will be
		filtered out from the resulting ARRAY.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Blob2ArrayUInt32(databand_1, false) AS data_array1;
		SELECT RT_Blob2ArrayInt32(databand_1, false) AS data_array2;
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		static const std::vector<std::pair<string, LogicalType>> type_variants = {
		    {"RT_Blob2ArrayUInt8", LogicalType::UTINYINT},   {"RT_Blob2ArrayInt8", LogicalType::TINYINT},
		    {"RT_Blob2ArrayUInt16", LogicalType::USMALLINT}, {"RT_Blob2ArrayInt16", LogicalType::SMALLINT},
		    {"RT_Blob2ArrayUInt32", LogicalType::UINTEGER},  {"RT_Blob2ArrayInt32", LogicalType::INTEGER},
		    {"RT_Blob2ArrayUInt64", LogicalType::UBIGINT},   {"RT_Blob2ArrayInt64", LogicalType::BIGINT},
		    {"RT_Blob2ArrayFloat", LogicalType::FLOAT},      {"RT_Blob2ArrayDouble", LogicalType::DOUBLE},
		};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const string &function_name = entry.first;
			const LogicalType &logical_type = entry.second;

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Array::Blob2Array(args, state, result, logical_type);
			};
			const ScalarFunction function(function_name, {LogicalType::BLOB, LogicalType::BOOLEAN},
			                              RasterTypes::ARRAY(logical_type), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
			                                 tags);
		}
	}
};

} // namespace

// #####################################################################################################################
// Register Array Function
// #####################################################################################################################

void RasterArrayFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Array::Register(loader);
}

} // namespace duckdb
