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

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.ChangeType(data_type);

			result.SetValue(i, arg_cube.ToBlob());
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
	static void ApplyUnaryOp(CubeUnaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		const CubeUnaryCellFunc func = [op](const CubeCellValue &value, double &result) {
			return CubeUnaryOp::Eval(op, value, result);
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			DataCube::Apply(func, arg_cube, res_cube);

			result.SetValue(i, res_cube.ToBlob());
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to two data cubes.
	static void ApplyBinaryOp1(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube_a(Allocator::Get(state.GetContext()));
		DataCube arg_cube_b(Allocator::Get(state.GetContext()));
		DataCube tmp_cube_r(Allocator::Get(state.GetContext()));

		const CubeBinaryCellFunc func = [op](const CubeCellValue &a, const CubeCellValue &b, double &result) {
			return CubeBinaryOp::Eval(op, a, b, result);
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob_a = args.data[0].GetValue(i);
			Value blob_b = args.data[1].GetValue(i);

			arg_cube_a.LoadBlob(blob_a);
			arg_cube_b.LoadBlob(blob_b);
			DataCube::Apply(func, arg_cube_a, arg_cube_b, tmp_cube_r);

			result.SetValue(i, tmp_cube_r.ToBlob());
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to a data cube and a scalar value.
	static void ApplyBinaryOp2(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		const CubeBinaryCellFunc func = [op](const CubeCellValue &a, const CubeCellValue &b, double &result) {
			return CubeBinaryOp::Eval(op, a, b, result);
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			const double value_b = args.data[1].GetValue(i).GetValue<double>();

			arg_cube.LoadBlob(blob);
			DataCube::Apply(func, arg_cube, value_b, res_cube);

			result.SetValue(i, res_cube.ToBlob());
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
		static constexpr std::array<std::tuple<const char *, CubeUnaryOp::Value, const char *>, 5> unary_ops = {{
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
		static constexpr std::array<std::tuple<const char *, CubeBinaryOp::Value, const char *>, 22> binary_ops = {{
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

		    {"RT_CubeSet", CubeBinaryOp::SET,
		     "Set the values in the data cube to the values in other data cube or a scalar value element-wise."},
		    {"RT_CubeMin", CubeBinaryOp::MIN,
		     "Take the minimum of the values in the data cube and other data cube or a scalar value element-wise."},
		    {"RT_CubeMax", CubeBinaryOp::MAX,
		     "Take the maximum of the values in the data cube and other data cube or a scalar value element-wise."},
		    {"RT_CubeFill", CubeBinaryOp::FILL,
		     "Set the values in the data cube without any validity check to the values in other data cube or a scalar "
		     "value element-wise."},

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
