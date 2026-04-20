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

//! Restore the result vector to CONSTANT_VECTOR if all input vectors are CONSTANT_VECTOR.
//! This is necessary to maintain the expected behavior in DuckDB when all arguments are
//! literals.
static void RestoreConstantIfNeeded(const DataChunk &args, Vector &result) {
	for (idx_t j = 0; j < args.data.size(); j++) {
		if (args.data[j].GetVectorType() != VectorType::CONSTANT_VECTOR) {
			return;
		}
	}
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
}

//======================================================================================================================
// RT_Cube2Array
//======================================================================================================================

struct RT_Cube2Array {
	//! Transform a BLOB data band into an ARRAY of values.
	static void Cube2Array(DataChunk &args, ExpressionState &state, Vector &result, const LogicalType &type) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube raw_cube(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because BinaryExecutor only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			bool filter_nodata = args.data[1].GetValue(i).GetValue<bool>();

			arg_cube.LoadBlob(blob);

			if (arg_cube.GetHeader().data_format != DataFormat::RAW) {
				arg_cube.ChangeFormat(DataFormat::RAW, raw_cube);
				result.SetValue(i, raw_cube.ToArray(type, filter_nodata));
			} else {
				result.SetValue(i, arg_cube.ToArray(type, filter_nodata));
			}
		}
		RestoreConstantIfNeeded(args, result);
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
	//! Transform an ARRAY of values into a BLOB data band.
	static void Array2Cube(const LogicalType &type, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
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
		RestoreConstantIfNeeded(args, result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Transforms an ARRAY of values into a BLOB data band.

		The function takes an ARRAY of values representing a raster tile, along with metadata parameters (e.g. dimensions,
		data format, nodata value), and converts them into a BLOB that encodes the raw tile data and its metadata.

		The resulting BLOB can be stored in a DuckDB table and later transformed back into an ARRAY using the
		RT_Cube2Array function.

		The parameters are as follows:
		- `data_format`: The data format to pack the values in the array (e.g. 'RAW').
		- `bands`: The number of bands or layers in the raster tile.
		- `cols`: The number of columns in the tile.
		- `rows`: The number of rows in the tile.
		- `nodata_value`: A numeric value representing the no-data value for the tile.
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
