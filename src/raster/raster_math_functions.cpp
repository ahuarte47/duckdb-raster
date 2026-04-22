#include "raster_math_functions.hpp"
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
// RT_ChangeType
//======================================================================================================================

struct RT_ChangeType {
	//! Change the data type of a data cube.
	static void Apply(const LogicalType &logicalType, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();

		const DataType::Value data_type = RasterUtils::LogicalTypeToDataType(logicalType);

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube raw_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);

			const DataHeader header = arg_cube.GetHeader();
			if (header.data_format != DataFormat::RAW) {
				arg_cube.ChangeFormat(DataFormat::RAW, raw_cube);
				raw_cube.ChangeType(data_type, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			} else {
				arg_cube.ChangeType(data_type, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			}
		}
		RestoreConstantIfNeeded(args, result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Changes the data type of a data cube, converting the data buffer accordingly.

		The output data cube will have the same dimensions and data format as the input cube, but with the
		specified data type.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Cube2TypeUInt8(databand_1 + databand_2) AS r FROM RT_Read('some/file/path/filename.tif);
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
				RT_ChangeType::Apply(logical_type, args, state, result);
			};
			const ScalarFunction function(function_name, {RasterTypes::DATACUBE()}, RasterTypes::DATACUBE(), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
			                                 tags);
		}
	}
};

//======================================================================================================================
// RT_Math
//======================================================================================================================

struct RT_Math {
	//! Apply a unary operation to a data cube.
	static void ApplyUnaryOp(const CubeUnaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube raw_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			arg_cube.LoadBlob(blob);

			const DataHeader header = arg_cube.GetHeader();
			if (header.data_format != DataFormat::RAW) {
				arg_cube.ChangeFormat(DataFormat::RAW, raw_cube);
				DataCube::Apply(op, raw_cube, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			} else {
				DataCube::Apply(op, arg_cube, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			}
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to two data cubes.
	static void ApplyBinaryOp1(const CubeBinaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube_a(Allocator::Get(state.GetContext()));
		DataCube arg_cube_b(Allocator::Get(state.GetContext()));
		DataCube raw_cube_a(Allocator::Get(state.GetContext()));
		DataCube raw_cube_b(Allocator::Get(state.GetContext()));
		DataCube res_cube__(Allocator::Get(state.GetContext()));
		DataCube *cube_a = nullptr;
		DataCube *cube_b = nullptr;

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob_a = args.data[0].GetValue(i);
			Value blob_b = args.data[1].GetValue(i);

			arg_cube_a.LoadBlob(blob_a);
			arg_cube_b.LoadBlob(blob_b);

			const DataHeader header_a = arg_cube_a.GetHeader();
			if (header_a.data_format != DataFormat::RAW) {
				arg_cube_a.ChangeFormat(DataFormat::RAW, raw_cube_a);
				cube_a = &raw_cube_a;
			} else {
				cube_a = &arg_cube_a;
			}

			const DataHeader header_b = arg_cube_b.GetHeader();
			if (header_b.data_format != DataFormat::RAW) {
				arg_cube_b.ChangeFormat(DataFormat::RAW, raw_cube_b);
				cube_b = &raw_cube_b;
			} else {
				cube_b = &arg_cube_b;
			}

			DataCube::Apply(op, *cube_a, *cube_b, res_cube__);
			result.SetValue(i, res_cube__.ToBlob());
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to a data cube and a scalar value.
	static void ApplyBinaryOp2(const CubeBinaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube raw_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			const double value_b = args.data[1].GetValue(i).GetValue<double>();

			const DataHeader header = arg_cube.GetHeader();
			if (header.data_format != DataFormat::RAW) {
				arg_cube.ChangeFormat(DataFormat::RAW, raw_cube);
				DataCube::Apply(op, raw_cube, value_b, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			} else {
				DataCube::Apply(op, arg_cube, value_b, res_cube);
				result.SetValue(i, res_cube.ToBlob());
			}
		}
		RestoreConstantIfNeeded(args, result);
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		// Register unary operations
		static constexpr std::array<std::tuple<const char *, CubeUnaryOp, const char *>, 5> unary_ops = {{
		    {"RT_CubeNeg", CubeUnaryOp::NEGATE, "Negate the values in the data cube element-wise."},
		    {"RT_CubeAbs", CubeUnaryOp::ABSOLUTE, "Absolute value of the values in the data cube element-wise."},
		    {"RT_CubeSqrt", CubeUnaryOp::SQUARE_ROOT, "Square root of the values in the data cube element-wise."},
		    {"RT_CubeLog", CubeUnaryOp::LOGARITHM, "Logarithm of the values in the data cube element-wise."},
		    {"RT_CubeExp", CubeUnaryOp::EXPONENTIAL, "Exponential of the values in the data cube element-wise."},
		}};
		for (const auto &entry : unary_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);

			const auto executor = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ApplyUnaryOp(op, args, state, result);
			};
			ScalarFunction function =
			    ScalarFunction(function_name, {RasterTypes::DATACUBE()}, RasterTypes::DATACUBE(), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, description, "",
			                                 tags);
		}

		// Register binary operations
		static constexpr std::array<std::tuple<const char *, CubeBinaryOp, const char *>, 18> binary_ops = {{
		    {"RT_CubeEqual", CubeBinaryOp::EQUAL,
		     "Return 1 where values in datacube_a are equal to datacube_b or a scalar value, 0 otherwise."},
		    {"RT_CubeNotEqual", CubeBinaryOp::NOT_EQUAL,
		     "Return 1 where values in datacube_a are not equal to datacube_b or a scalar value, 0 otherwise."},
		    {"RT_CubeGreater", CubeBinaryOp::GREATER,
		     "Return 1 where values in datacube_a are greater than datacube_b or a scalar value, 0 otherwise."},
		    {"RT_CubeLess", CubeBinaryOp::LESS,
		     "Return 1 where values in datacube_a are less than datacube_b or a scalar value, 0 otherwise."},
		    {"RT_CubeGreaterEqual", CubeBinaryOp::GREATER_EQUAL,
		     "Return 1 where values in datacube_a are greater than or equal to datacube_b or a scalar value, 0 "
		     "otherwise."},
		    {"RT_CubeLessEqual", CubeBinaryOp::LESS_EQUAL,
		     "Return 1 where values in datacube_a are less than or equal to datacube_b or a scalar value, 0 "
		     "otherwise."},

		    {"RT_CubeAdd", CubeBinaryOp::ADD, "Add to data cube other data cube or a scalar value element-wise."},
		    {"RT_CubeSubtract", CubeBinaryOp::SUBTRACT,
		     "Subtract from data cube other data cube or a scalar value element-wise."},
		    {"RT_CubeMultiply", CubeBinaryOp::MULTIPLY,
		     "Multiply data cube by other data cube or a scalar value element-wise."},
		    {"RT_CubeDivide", CubeBinaryOp::DIVIDE,
		     "Divide data cube by other data cube or a scalar value element-wise."},
		    {"RT_CubePow", CubeBinaryOp::POW,
		     "Raise data cube to the power of other data cube or a scalar value element-wise."},
		    {"RT_CubeMod", CubeBinaryOp::MOD,
		     "Modulus of data cube by other data cube or a scalar value element-wise."},

		    {"+", CubeBinaryOp::ADD, "Add to data cube other data cube or a scalar value element-wise."},
		    {"-", CubeBinaryOp::SUBTRACT, "Subtract from data cube other data cube or a scalar value element-wise."},
		    {"*", CubeBinaryOp::MULTIPLY, "Multiply data cube by other data cube or a scalar value element-wise."},
		    {"/", CubeBinaryOp::DIVIDE, "Divide data cube by other data cube or a scalar value element-wise."},
		    {"^", CubeBinaryOp::POW, "Raise data cube to the power of other data cube or a scalar value element-wise."},
		    {"%", CubeBinaryOp::MOD, "Modulus of data cube by other data cube or a scalar value element-wise."},
		}};
		for (const auto &entry : binary_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);

			ScalarFunctionSet function_set(function_name);

			const auto executor01 = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ApplyBinaryOp1(op, args, state, result);
			};
			ScalarFunction func01 = ScalarFunction(function_name, {RasterTypes::DATACUBE(), RasterTypes::DATACUBE()},
			                                       RasterTypes::DATACUBE(), executor01);

			function_set.AddFunction(func01);

			const auto executor02 = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ApplyBinaryOp2(op, args, state, result);
			};
			ScalarFunction func02 = ScalarFunction(function_name, {RasterTypes::DATACUBE(), LogicalType::DOUBLE},
			                                       RasterTypes::DATACUBE(), executor02);

			function_set.AddFunction(func02);

			RegisterFunction<ScalarFunctionSet>(loader, function_set, CatalogType::SCALAR_FUNCTION_ENTRY, description,
			                                    "", tags);
		}
	}
};

} // namespace

// #####################################################################################################################
// Register Math Functions
// #####################################################################################################################

void RasterMathFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_ChangeType::Register(loader);
	RT_Math::Register(loader);
}

} // namespace duckdb
