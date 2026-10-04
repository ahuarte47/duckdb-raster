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

//======================================================================================================================
// RT_NullOrEmpty
//======================================================================================================================

struct RT_NullOrEmpty {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Returns true if the data cube is null or empty, false otherwise.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);

			result.SetValue(i, Value::BOOLEAN(arg_cube.IsNullOrEmpty()));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns `true` if the datacube is `NULL` or contains only nodata cells, `false` otherwise.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CubeNullOrEmpty(databand_1) FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		const ScalarFunction function("RT_CubeNullOrEmpty", {RasterTypes::DATACUBE()}, LogicalType::BOOLEAN, Execute);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//======================================================================================================================
// RT_Math
//======================================================================================================================

struct RT_Math {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Execute a unary operation to a datacube.
	static void ExecuteUnaryOp(CubeUnaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

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
	}

	//! Execute a binary operation on two datacubes.
	static void ExecuteBinaryOp1a(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

		DataCube arg_cube_a(Allocator::Get(state.GetContext()));
		DataCube arg_cube_b(Allocator::Get(state.GetContext()));
		DataCube tmp_cube_r(Allocator::Get(state.GetContext()));
		double tmp_b = 0;

		const CubeBinaryCellFunc func = [op, &tmp_b](const CubeCellValue &a, const CubeCellValue &b, double &result) {
			tmp_b = b.value; // Store the right-hand value in a temporary variable to be used in SET_NODATA operation.
			return CubeBinaryOp::Eval(op, a, b, result);
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob_a = args.data[0].GetValue(i);
			Value blob_b = args.data[1].GetValue(i);

			arg_cube_a.LoadBlob(blob_a);
			arg_cube_b.LoadBlob(blob_b);
			DataCube::Apply(func, arg_cube_a, arg_cube_b, tmp_cube_r);

			// For SET_NODATA, we need to update the header of the resulting cube to set the new nodata value.
			if (op == CubeBinaryOp::SET_NODATA) {
				const DataHeader header = arg_cube_a.GetHeader();
				DataHeader new_header = header;
				new_header.no_data = tmp_b;
				auto old_position = tmp_cube_r.GetBuffer().GetPosition();
				tmp_cube_r.SetHeader(new_header, false);
				tmp_cube_r.GetBuffer().SetPosition(old_position);
			}

			result.SetValue(i, tmp_cube_r.ToBlob());
		}
	}

	//! Execute a binary operation on two datacubes (redefining the nodata value in the result).
	static void ExecuteBinaryOp1b(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 3);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

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
			const double r_nodata = args.data[2].GetValue(i).GetValue<double>();

			arg_cube_a.LoadBlob(blob_a);
			arg_cube_b.LoadBlob(blob_b);
			DataCube::Apply(func, arg_cube_a, arg_cube_b, tmp_cube_r, r_nodata);

			result.SetValue(i, tmp_cube_r.ToBlob());
		}
	}

	//! Execute a binary operation on a datacube and a scalar value.
	static void ExecuteBinaryOp2a(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

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

			// For SET_NODATA, we need to update the header of the resulting cube to set the new nodata value.
			if (op == CubeBinaryOp::SET_NODATA) {
				const DataHeader header = arg_cube.GetHeader();
				DataHeader new_header = header;
				new_header.no_data = value_b;
				auto old_position = res_cube.GetBuffer().GetPosition();
				res_cube.SetHeader(new_header, false);
				res_cube.GetBuffer().SetPosition(old_position);
			}

			result.SetValue(i, res_cube.ToBlob());
		}
	}

	//! Execute a binary operation on a datacube and a scalar value (redefining the nodata value in the result).
	static void ExecuteBinaryOp2b(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 3);
		const idx_t count = args.size();
		args.Flatten();

		// Set the result vector type to constant if there is only one input row.
		if (count == 1) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
		}

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		const CubeBinaryCellFunc func = [op](const CubeCellValue &a, const CubeCellValue &b, double &result) {
			return CubeBinaryOp::Eval(op, a, b, result);
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			const double value_b = args.data[1].GetValue(i).GetValue<double>();
			const double r_nodata = args.data[2].GetValue(i).GetValue<double>();

			arg_cube.LoadBlob(blob);
			DataCube::Apply(func, arg_cube, value_b, res_cube, r_nodata);

			result.SetValue(i, res_cube.ToBlob());
		}
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
		    {"RT_CubeNeg", CubeUnaryOp::NEGATE,
		     "Returns a datacube with each cell negated (multiplied by -1). Nodata cells are preserved."},
		    {"RT_CubeAbs", CubeUnaryOp::ABSOLUTE,
		     "Returns a datacube with the absolute value of each cell. Nodata cells are preserved."},
		    {"RT_CubeSqrt", CubeUnaryOp::SQUARE_ROOT,
		     "Returns a datacube with the square root of each cell. Nodata cells are preserved."},
		    {"RT_CubeLog", CubeUnaryOp::LOGARITHM,
		     "Returns a datacube with the natural logarithm of each cell. Nodata cells are preserved."},
		    {"RT_CubeExp", CubeUnaryOp::EXPONENTIAL,
		     "Returns a datacube with the exponential (e^x) of each cell. Nodata cells are preserved."},
		}};
		for (const auto &entry : unary_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);

			const auto executor = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ExecuteUnaryOp(op, args, state, result);
			};
			ScalarFunction function =
			    ScalarFunction(function_name, {RasterTypes::DATACUBE()}, RasterTypes::DATACUBE(), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, description, "",
			                                 tags);
		}

		// Register binary operations
		static constexpr std::array<std::tuple<const char *, CubeBinaryOp::Value, const char *, bool>, 23> binary_ops =
		    {{
		        // Arithmetic
		        {"RT_CubeAdd", CubeBinaryOp::ADD,
		         "Returns a data cube with each cell equal to the sum of the corresponding cells of the two inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeSubtract", CubeBinaryOp::SUBTRACT,
		         "Returns a data cube with each cell equal to the left-hand cell minus the right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeMultiply", CubeBinaryOp::MULTIPLY,
		         "Returns a data cube with each cell equal to the product of the corresponding cells of the two "
		         "inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeDivide", CubeBinaryOp::DIVIDE,
		         "Returns a data cube with each cell equal to the left-hand cell divided by the right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubePow", CubeBinaryOp::POW,
		         "Returns a data cube with each cell raised to the power of the corresponding right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeMod", CubeBinaryOp::MOD,
		         "Returns a data cube with each cell equal to the remainder of dividing the left-hand cell by the "
		         "right-hand cell. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         true},
		        // Comparison
		        {"RT_CubeEqual", CubeBinaryOp::EQUAL,
		         "Returns a data cube where each cell is 1 if the corresponding cells of the two inputs are equal, "
		         "0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeNotEqual", CubeBinaryOp::NOT_EQUAL,
		         "Returns a data cube where each cell is 1 if the corresponding cells of the two inputs differ, "
		         "0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeLess", CubeBinaryOp::LESS,
		         "Returns a data cube where each cell is 1 if the left-hand cell is strictly less than the right-hand "
		         "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         true},
		        {"RT_CubeLessEqual", CubeBinaryOp::LESS_EQUAL,
		         "Returns a data cube where each cell is 1 if the left-hand cell is less than or equal to the "
		         "right-hand "
		         "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         true},
		        {"RT_CubeGreater", CubeBinaryOp::GREATER,
		         "Returns a data cube where each cell is 1 if the left-hand cell is strictly greater than the "
		         "right-hand "
		         "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         true},
		        {"RT_CubeGreaterEqual", CubeBinaryOp::GREATER_EQUAL,
		         "Returns a data cube where each cell is 1 if the left-hand cell is greater than or equal to the "
		         "right-hand "
		         "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         true},
		        // Assignment / utility
		        {"RT_CubeSet", CubeBinaryOp::SET,
		         "Returns a data cube where valid cells are replaced by the corresponding values of a second data cube "
		         "or "
		         "scalar. No-data cells in the source are preserved.",
		         true},
		        {"RT_CubeSetNoData", CubeBinaryOp::SET_NODATA,
		         "Returns a data cube where no-data cells are replaced by the specified value. Valid cells in the "
		         "source "
		         "are preserved.",
		         true},
		        {"RT_CubeFill", CubeBinaryOp::FILL,
		         "Returns a data cube where all cells (including no-data) are unconditionally replaced by the "
		         "corresponding values of a second data cube or scalar, bypassing no-data checks.",
		         true},
		        {"RT_CubeMin", CubeBinaryOp::MIN,
		         "Returns a data cube with each cell equal to the minimum of the corresponding cells of the two "
		         "inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        {"RT_CubeMax", CubeBinaryOp::MAX,
		         "Returns a data cube with each cell equal to the maximum of the corresponding cells of the two "
		         "inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         true},
		        // Arithmetic operators
		        {"+", CubeBinaryOp::ADD,
		         "Returns a data cube with each cell equal to the sum of the corresponding cells of the two inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         false},
		        {"-", CubeBinaryOp::SUBTRACT,
		         "Returns a data cube with each cell equal to the left-hand cell minus the right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         false},
		        {"*", CubeBinaryOp::MULTIPLY,
		         "Returns a data cube with each cell equal to the product of the corresponding cells of the two "
		         "inputs. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         false},
		        {"/", CubeBinaryOp::DIVIDE,
		         "Returns a data cube with each cell equal to the left-hand cell divided by the right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         false},
		        {"^", CubeBinaryOp::POW,
		         "Returns a data cube with each cell raised to the power of the corresponding right-hand cell. "
		         "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved.",
		         false},
		        {"%", CubeBinaryOp::MOD,
		         "Returns a data cube with each cell equal to the remainder of dividing the left-hand cell by the "
		         "right-hand cell. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		         "preserved.",
		         false},
		    }};
		for (const auto &entry : binary_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);
			const auto &override_function = std::get<3>(entry);

			ScalarFunctionSet function_set(function_name);

			const auto executor1a = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ExecuteBinaryOp1a(op, args, state, result);
			};
			ScalarFunction func1a = ScalarFunction(function_name, {RasterTypes::DATACUBE(), RasterTypes::DATACUBE()},
			                                       RasterTypes::DATACUBE(), executor1a);

			function_set.AddFunction(func1a);

			const auto executor2a = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_Math::ExecuteBinaryOp2a(op, args, state, result);
			};
			ScalarFunction func2a = ScalarFunction(function_name, {RasterTypes::DATACUBE(), LogicalType::DOUBLE},
			                                       RasterTypes::DATACUBE(), executor2a);

			function_set.AddFunction(func2a);

			// Override with extra parameter for specifying the nodata value of the result
			if (override_function) {
				const auto executor1b = [op](DataChunk &args, ExpressionState &state, Vector &result) {
					RT_Math::ExecuteBinaryOp1b(op, args, state, result);
				};
				ScalarFunction func1b = ScalarFunction(
				    function_name, {RasterTypes::DATACUBE(), RasterTypes::DATACUBE(), LogicalType::DOUBLE},
				    RasterTypes::DATACUBE(), executor1b);

				function_set.AddFunction(func1b);

				const auto executor2b = [op](DataChunk &args, ExpressionState &state, Vector &result) {
					RT_Math::ExecuteBinaryOp2b(op, args, state, result);
				};
				ScalarFunction func2b =
				    ScalarFunction(function_name, {RasterTypes::DATACUBE(), LogicalType::DOUBLE, LogicalType::DOUBLE},
				                   RasterTypes::DATACUBE(), executor2b);

				function_set.AddFunction(func2b);
			}

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
	RT_NullOrEmpty::Register(loader);
	RT_Math::Register(loader);
}

} // namespace duckdb
