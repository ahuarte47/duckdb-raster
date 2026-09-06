#include "raster_format_functions.hpp"
#include "raster_utils.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/scalar_function.hpp"
// GDAL
#include "modules/gdal_dataset_io.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// RT_ChangeType
//======================================================================================================================

struct RT_ChangeType {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Change the data type of a data cube.
	static void Execute(const LogicalType &logicalType, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

		const DataType::Value data_type = RasterUtils::LogicalTypeToDataType(logicalType);

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.ChangeType(data_type);

			result.SetValue(i, arg_cube.ToBlob());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Changes the pixel data type of a datacube, returning a new datacube of the same dimensions.

		All arithmetic operations produce `DOUBLE` values internally. Use this function to convert the result to the desired storage type before writing to a raster file, or to reinterpret the data type of an existing band (e.g. from `INT16` to `FLOAT`).

		Function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `datacube` | DATACUBE | The datacube column whose pixel type will be converted. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Cube2TypeUInt8(databand_1 + databand_2) AS r FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		static constexpr std::array<std::pair<const char *, LogicalTypeId>, 10> type_variants = {{
		    {"RT_Cube2TypeUInt8", LogicalTypeId::UTINYINT},
		    {"RT_Cube2TypeInt8", LogicalTypeId::TINYINT},
		    {"RT_Cube2TypeUInt16", LogicalTypeId::USMALLINT},
		    {"RT_Cube2TypeInt16", LogicalTypeId::SMALLINT},
		    {"RT_Cube2TypeUInt32", LogicalTypeId::UINTEGER},
		    {"RT_Cube2TypeInt32", LogicalTypeId::INTEGER},
		    {"RT_Cube2TypeUInt64", LogicalTypeId::UBIGINT},
		    {"RT_Cube2TypeInt64", LogicalTypeId::BIGINT},
		    {"RT_Cube2TypeFloat", LogicalTypeId::FLOAT},
		    {"RT_Cube2TypeDouble", LogicalTypeId::DOUBLE},
		}};

		// Register a separate function for each supported array type.
		for (const auto &entry : type_variants) {
			const char *function_name = entry.first;
			const LogicalType logical_type(entry.second);

			const auto executor = [logical_type](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_ChangeType::Execute(logical_type, args, state, result);
			};
			const ScalarFunction function(function_name, {RasterTypes::DATACUBE()}, RasterTypes::DATACUBE(), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
			                                 tags);
		}
	}
};

//======================================================================================================================
// RT_Metadata
//======================================================================================================================

struct RT_Metadata {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Retrieve the metadata of a raster.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

		auto &client_context = state.GetContext();
		GDALDatasetUniquePtr dataset;
		std::string dataset_path;

		for (idx_t i = 0; i < count; i++) {
			const std::string file_path = args.data[0].GetValue(i).GetValue<string>();

			// Open the dataset if the file path has changed.

			if (dataset_path != file_path) {
				dataset = GDALDatasetUniquePtr(DuckDBDatasetFactory::OpenDataset(client_context, {file_path}, {}));
				dataset_path = file_path;
			}

			// Extract the metadata from the dataset.

			std::string metadata = RasterUtils::GetMetadataOfDataset(dataset.get());
			result.SetValue(i, metadata);
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Retrieves the metadata of a raster file, returning it as a JSON string.

		Function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `filepath` | VARCHAR | The path to the raster file whose metadata will be retrieved. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Metadata('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		ScalarFunction function("RT_Metadata", {LogicalType::VARCHAR}, LogicalType::JSON(), Execute);
		function.SetVolatile();

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

} // namespace

// #####################################################################################################################
// Register Format Functions
// #####################################################################################################################

void RasterFormatFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_ChangeType::Register(loader);
	RT_Metadata::Register(loader);
}

} // namespace duckdb
