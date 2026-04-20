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
// RT_Math
//======================================================================================================================

struct RT_Math {
	//! Apply a unary operation to a data cube.
	static void ApplyUnaryOp(const CubeUnaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();

		DataCube cube_a(Allocator::Get(state.GetContext()));
		DataCube cube_r(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			if (cube_a.LoadBlob(args.data[0].GetValue(i)) > 0) {
				DataCube::Apply(op, cube_a, cube_r);
				result.SetValue(i, cube_r.ToBlob());
			} else {
				result.SetValue(i, DataCube::EMPTY_CUBE().ToBlob());
			}
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to two data cubes.
	static void ApplyBinaryOp1(const CubeBinaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube cube_a(Allocator::Get(state.GetContext()));
		DataCube cube_b(Allocator::Get(state.GetContext()));
		DataCube cube_r(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			if (cube_a.LoadBlob(args.data[0].GetValue(i)) > 0 && cube_b.LoadBlob(args.data[1].GetValue(i)) > 0) {
				DataCube::Apply(op, cube_a, cube_b, cube_r);
				result.SetValue(i, cube_r.ToBlob());
			} else {
				result.SetValue(i, DataCube::EMPTY_CUBE().ToBlob());
			}
		}
		RestoreConstantIfNeeded(args, result);
	}

	//! Apply a binary operation to a data cube and a scalar value.
	static void ApplyBinaryOp2(const CubeBinaryOp op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube cube_a(Allocator::Get(state.GetContext()));
		DataCube cube_r(Allocator::Get(state.GetContext()));

		// We loop over the rows by hand because Executors only supports C++ primitives.
		for (idx_t i = 0; i < count; i++) {
			if (cube_a.LoadBlob(args.data[0].GetValue(i)) > 0) {
				const double value_b = args.data[1].GetValue(i).GetValue<double>();
				DataCube::Apply(op, cube_a, value_b, cube_r);
				result.SetValue(i, cube_r.ToBlob());
			} else {
				result.SetValue(i, DataCube::EMPTY_CUBE().ToBlob());
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
	RT_Math::Register(loader);
}

} // namespace duckdb
