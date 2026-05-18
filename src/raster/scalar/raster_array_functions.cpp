#include "raster_array_functions.hpp"
#include "raster_utils.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// RT_Array2Cube
//======================================================================================================================

struct RT_Array2Cube {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Serializes an ARRAY of pixel values plus metadata into a datacube BLOB.
	static void Execute(const LogicalType &type, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value in_array = args.data[0].GetValue(i);

			const int32_t bands = args.data[2].GetValue(i).GetValue<int32_t>();
			if (bands < 0) {
				throw InvalidInputException("Number of bands cannot be negative");
			}

			const int32_t cols = args.data[3].GetValue(i).GetValue<int32_t>();
			if (cols < 0) {
				throw InvalidInputException("Number of columns cannot be negative");
			}

			const int32_t rows = args.data[4].GetValue(i).GetValue<int32_t>();
			if (rows < 0) {
				throw InvalidInputException("Number of rows cannot be negative");
			}

			const DataHeader in_header = {DataFormat::FromString(args.data[1].GetValue(i).GetValue<string>()),
			                              RasterUtils::LogicalTypeToDataType(type),
			                              bands,
			                              cols,
			                              rows,
			                              args.data[5].GetValue(i).GetValue<double>()};

			arg_cube.LoadArray(in_array, in_header);

			result.SetValue(i, arg_cube.ToBlob());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Packages a plain SQL array of numeric values back into a datacube BLOB, the inverse of `RT_Cube2Array`.

		Use this function to convert the output of array-level operations (e.g. `list_transform`, custom UDFs) back into a datacube that can be written to a raster file with `COPY … FORMAT RASTER`.

		Function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `in_array` | ARRAY | The array of numeric values to transform into a BLOB column. |
		| `data_format` | VARCHAR | The data format to use for packing the data into the BLOB. |
		| `bands` | INT | Number of bands or layers in the data buffer. |
		| `cols` | INT | Number of columns in the tile. |
		| `rows` | INT | Number of rows in the tile. |
		| `no_data` | DOUBLE | NoData value for the tile (to be considered when applying algebraic operations). |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Array2Cube([1,2,3,4]::INTEGER[], 'RAW', 1, 4, 1, -9999.0);
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		ScalarFunctionSet function_set("RT_Array2Cube");

		static constexpr std::array<std::pair<const char *, LogicalTypeId>, 10> type_variants = {{
		    {"RT_Array2Cube", LogicalTypeId::UTINYINT},
		    {"RT_Array2Cube", LogicalTypeId::TINYINT},
		    {"RT_Array2Cube", LogicalTypeId::USMALLINT},
		    {"RT_Array2Cube", LogicalTypeId::SMALLINT},
		    {"RT_Array2Cube", LogicalTypeId::UINTEGER},
		    {"RT_Array2Cube", LogicalTypeId::INTEGER},
		    {"RT_Array2Cube", LogicalTypeId::UBIGINT},
		    {"RT_Array2Cube", LogicalTypeId::BIGINT},
		    {"RT_Array2Cube", LogicalTypeId::FLOAT},
		    {"RT_Array2Cube", LogicalTypeId::DOUBLE},
		}};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const char *function_name = entry.first;
			const LogicalType logical_type(entry.second);

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Array2Cube::Execute(logical_type, args, state, result);
			};
			const ScalarFunction function(function_name,
			                              {LogicalType::LIST(logical_type), LogicalType::VARCHAR, LogicalType::INTEGER,
			                               LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::DOUBLE},
			                              RasterTypes::DATACUBE(), executor);

			function_set.AddFunction(function);
		}

		RegisterFunction<ScalarFunctionSet>(loader, function_set, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION,
		                                    EXAMPLE, tags);
	}
};

//======================================================================================================================
// RT_Cube2Array
//======================================================================================================================

struct RT_Cube2Array {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Deserializes a datacube BLOB into a structured ARRAY value with metadata and pixel values.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result, const LogicalType &type) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			bool filter_nodata = args.data[1].GetValue(i).GetValue<bool>();

			arg_cube.LoadBlob(blob);
			Value array = arg_cube.ToArray(type, filter_nodata);

			result.SetValue(i, std::move(array));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Extracts the pixel values of a datacube column into a plain SQL array of a chosen numeric type.

		Function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `datacube` | DATACUBE | The datacube column to extract values from. |
		| `filter_nodata` | BOOLEAN | When `true`, nodata cells are excluded from the output array. |

		Functions return a struct with the following fields:

		+ `data_type` (INT): Numeric data type code of the source datacube.
		+ `bands` (INT): Number of bands in the tile.
		+ `cols` (INT): Number of pixel columns in the tile.
		+ `rows` (INT): Number of pixel rows in the tile.
		+ `no_data` (DOUBLE): Nodata sentinel value (`-infinity` when not defined).
		+ `values` (ARRAY): Flat array of pixel values in row-major order.

		A direct SQL cast (`::DOUBLE[]`, etc.) is also supported as a shorthand, but nodata values are not filtered in that case.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Cube2ArrayUInt32(databand_1, false) AS data_array1;
		SELECT RT_Cube2ArrayInt32(databand_1, false) AS data_array2;
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		static constexpr std::array<std::pair<const char *, LogicalTypeId>, 10> type_variants = {{
		    {"RT_Cube2ArrayUInt8", LogicalTypeId::UTINYINT},
		    {"RT_Cube2ArrayInt8", LogicalTypeId::TINYINT},
		    {"RT_Cube2ArrayUInt16", LogicalTypeId::USMALLINT},
		    {"RT_Cube2ArrayInt16", LogicalTypeId::SMALLINT},
		    {"RT_Cube2ArrayUInt32", LogicalTypeId::UINTEGER},
		    {"RT_Cube2ArrayInt32", LogicalTypeId::INTEGER},
		    {"RT_Cube2ArrayUInt64", LogicalTypeId::UBIGINT},
		    {"RT_Cube2ArrayInt64", LogicalTypeId::BIGINT},
		    {"RT_Cube2ArrayFloat", LogicalTypeId::FLOAT},
		    {"RT_Cube2ArrayDouble", LogicalTypeId::DOUBLE},
		}};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const char *function_name = entry.first;
			const LogicalType logical_type(entry.second);

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Cube2Array::Execute(args, state, result, logical_type);
			};
			const ScalarFunction function(function_name, {RasterTypes::DATACUBE(), LogicalType::BOOLEAN},
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
	RT_Array2Cube::Register(loader);
	RT_Cube2Array::Register(loader);
}

} // namespace duckdb
