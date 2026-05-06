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
// RT_Cube2Array
//======================================================================================================================

struct RT_Cube2Array {
	//! Deserializes a datacube BLOB into a structured ARRAY value with metadata and pixel values.
	static void Cube2Array(DataChunk &args, ExpressionState &state, Vector &result, const LogicalType &type) {
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

		The datacube BLOB encodes the raw pixel data of a raster tile together with its metadata
		(dimensions, data type, nodata value). The function reads that metadata and converts the
		pixel data into a flat SQL array.

		When `filter_nodata` is `true`, cells matching the nodata sentinel are excluded from the output array.
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
				RT_Cube2Array::Cube2Array(args, state, result, logical_type);
			};
			const ScalarFunction function(function_name, {RasterTypes::DATACUBE(), LogicalType::BOOLEAN},
			                              RasterTypes::ARRAY(logical_type), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
			                                 tags);
		}
	}
};

//======================================================================================================================
// RT_Array2Cube
//======================================================================================================================

struct RT_Array2Cube {
	//! Serializes an ARRAY of pixel values plus metadata into a datacube BLOB.
	static void Array2Cube(const LogicalType &type, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value in_array = args.data[0].GetValue(i);

			const DataHeader in_header = {DataFormat::FromString(args.data[1].GetValue(i).GetValue<string>()),
			                              RasterUtils::LogicalTypeToDataType(type),
			                              args.data[2].GetValue(i).GetValue<int32_t>(),
			                              args.data[3].GetValue(i).GetValue<int32_t>(),
			                              args.data[4].GetValue(i).GetValue<int32_t>(),
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

		The function takes a flat array of pixel values plus tile metadata and encodes them into the datacube
		BLOB format expected by `COPY ... FORMAT RASTER` and the other `RT_Cube*` functions.

		Parameters:
		- `data_format`: Compression format for the pixel data (e.g. 'RAW').
		- `bands`: Number of bands in the tile.
		- `cols`: Number of pixel columns.
		- `rows`: Number of pixel rows.
		- `no_data`: Nodata sentinel value for the tile.
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
				RT_Array2Cube::Array2Cube(logical_type, args, state, result);
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

} // namespace

// #####################################################################################################################
// Register Array Function
// #####################################################################################################################

void RasterArrayFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Cube2Array::Register(loader);
	RT_Array2Cube::Register(loader);
}

} // namespace duckdb
