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
// RT_Blob2Array
//======================================================================================================================

struct RT_Blob2Array {
	//! Transform a BLOB data band into an ARRAY of values.
	static void Blob2Array(DataChunk &args, ExpressionState &state, Vector &result, const LogicalType &type) {
		D_ASSERT(args.data.size() == 2);

		const idx_t count = args.size();

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

		static constexpr std::array<std::pair<const char *, LogicalTypeId>, 10> type_variants = {{
		    {"RT_Blob2ArrayUInt8", LogicalTypeId::UTINYINT},
		    {"RT_Blob2ArrayInt8", LogicalTypeId::TINYINT},
		    {"RT_Blob2ArrayUInt16", LogicalTypeId::USMALLINT},
		    {"RT_Blob2ArrayInt16", LogicalTypeId::SMALLINT},
		    {"RT_Blob2ArrayUInt32", LogicalTypeId::UINTEGER},
		    {"RT_Blob2ArrayInt32", LogicalTypeId::INTEGER},
		    {"RT_Blob2ArrayUInt64", LogicalTypeId::UBIGINT},
		    {"RT_Blob2ArrayInt64", LogicalTypeId::BIGINT},
		    {"RT_Blob2ArrayFloat", LogicalTypeId::FLOAT},
		    {"RT_Blob2ArrayDouble", LogicalTypeId::DOUBLE},
		}};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const char *function_name = entry.first;
			const LogicalType logical_type(entry.second);

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Blob2Array::Blob2Array(args, state, result, logical_type);
			};
			const ScalarFunction function(function_name, {LogicalType::BLOB, LogicalType::BOOLEAN},
			                              RasterTypes::ARRAY(logical_type), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
			                                 tags);
		}
	}
};

//======================================================================================================================
// RT_Array2Blob
//======================================================================================================================

struct RT_Array2Blob {
	//! Transform an ARRAY of values into a BLOB data band.
	static void Array2Blob(const LogicalType &type, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();

		MemoryStream data_buffer(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value in_array = args.data[0].GetValue(i);

			const TileHeader header = {CompressionAlg::FromString(args.data[1].GetValue(i).GetValue<string>()),
			                           RasterDataType::FromLogicalType(type),
			                           args.data[2].GetValue(i).GetValue<int32_t>(),
			                           args.data[3].GetValue(i).GetValue<int32_t>(),
			                           args.data[4].GetValue(i).GetValue<int32_t>(),
			                           args.data[5].GetValue(i).GetValue<double>()};

			data_buffer.SetPosition(0);

			std::size_t blob_size = RasterUtils::ArrayAsStream(in_array, header, data_buffer);
			if (blob_size == 0) {
				result.SetValue(i, Value::BLOB(""));
			} else {
				result.SetValue(i, Value::BLOB(data_buffer.GetData(), blob_size));
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Transforms an ARRAY of values into a BLOB data band.

		The function takes an ARRAY of values representing a raster tile, along with metadata parameters (e.g. dimensions,
		data type, nodata value), and converts them into a BLOB that encodes the raw tile data and its metadata.

		The resulting BLOB can be stored in a DuckDB table and later transformed back into an ARRAY using the
		RT_Blob2Array function.

		The metadata parameters are as follows:
		- `compression`: The compression algorithm to use for the tile data (e.g. 'none').
		- `bands`: The number of bands or layers in the raster tile.
		- `cols`: The number of columns in the tile.
		- `rows`: The number of rows in the tile.
		- `nodata_value`: A numeric value representing the no-data value for the tile.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Array2Blob([1,2,3,4]::INTEGER[], 'none', 1, 4, 1, -9999.0);
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		ScalarFunctionSet function_set("RT_Array2Blob");

		static constexpr std::array<std::pair<const char *, LogicalTypeId>, 10> type_variants = {{
		    {"RT_Array2Blob", LogicalTypeId::UTINYINT},
		    {"RT_Array2Blob", LogicalTypeId::TINYINT},
		    {"RT_Array2Blob", LogicalTypeId::USMALLINT},
		    {"RT_Array2Blob", LogicalTypeId::SMALLINT},
		    {"RT_Array2Blob", LogicalTypeId::UINTEGER},
		    {"RT_Array2Blob", LogicalTypeId::INTEGER},
		    {"RT_Array2Blob", LogicalTypeId::UBIGINT},
		    {"RT_Array2Blob", LogicalTypeId::BIGINT},
		    {"RT_Array2Blob", LogicalTypeId::FLOAT},
		    {"RT_Array2Blob", LogicalTypeId::DOUBLE},
		}};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const char *function_name = entry.first;
			const LogicalType logical_type(entry.second);

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Array2Blob::Array2Blob(logical_type, args, state, result);
			};
			const ScalarFunction function(function_name,
			                              {LogicalType::LIST(logical_type), LogicalType::VARCHAR, LogicalType::INTEGER,
			                               LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::DOUBLE},
			                              LogicalType::BLOB, executor);

			function_set.AddFunction(function);
		}

		RegisterFunction<ScalarFunctionSet>(loader, function_set, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION,
		                                    EXAMPLE, tags);
	}
};

} // namespace

// #####################################################################################################################
// Register Array Function
// #####################################################################################################################

void RasterArrayFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Blob2Array::Register(loader);
	RT_Array2Blob::Register(loader);
}

} // namespace duckdb
