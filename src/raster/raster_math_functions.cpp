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
// RT_Stats
//======================================================================================================================

struct RT_Stats {
	//! Statistics for a data cube band.
	struct CubeStats {
		int64_t valid_count = 0;
		int64_t nodata_count = 0;
		double min_val = NumericLimits<double>::Maximum();
		double max_val = NumericLimits<double>::Minimum();
		double sum = 0.0;
		double mean = 0.0;
		double m2 = 0.0;

		//! Update the statistics with a new cell value.
		void Update(const CubeCellValue &v) {
			if (v.IsValidValue()) {
				valid_count++;

				if (v.value < min_val) {
					min_val = v.value;
				}
				if (v.value > max_val) {
					max_val = v.value;
				}
				sum += v.value;

				// Welford's variance accumulator
				double delta = v.value - mean;
				mean += delta / valid_count;
				double delta2 = v.value - mean;
				m2 += delta * delta2;
			} else {
				nodata_count++;
			}
		}

		//! Update the statistics with another CubeStats (e.g. for combining results across multiple cubes).
		void Update(const CubeStats &other) {
			nodata_count += other.nodata_count;
			if (other.valid_count == 0) {
				return;
			}
			const int64_t combined_count = valid_count + other.valid_count;
			const double delta = other.mean - mean;
			m2 += other.m2 + delta * delta * static_cast<double>(valid_count) * static_cast<double>(other.valid_count) /
			                     static_cast<double>(combined_count);
			mean = (static_cast<double>(valid_count) * mean + static_cast<double>(other.valid_count) * other.mean) /
			       static_cast<double>(combined_count);
			valid_count = combined_count;

			if (other.min_val < min_val) {
				min_val = other.min_val;
			}
			if (other.max_val > max_val) {
				max_val = other.max_val;
			}
			sum += other.sum;
		}

		//! Compute the statistics as a DuckDB Value.
		Value ToValue() const {
			Value value = Value::STRUCT({{"minimum", Value::DOUBLE(valid_count > 0 ? min_val : 0.0)},
			                             {"maximum", Value::DOUBLE(valid_count > 0 ? max_val : 0.0)},
			                             {"sum", Value::DOUBLE(sum)},
			                             {"mean", Value::DOUBLE(mean)},
			                             {"stddev", Value::DOUBLE(valid_count > 0 ? std::sqrt(m2 / valid_count) : 0.0)},
			                             {"valid_count", Value::BIGINT(valid_count)},
			                             {"nodata_count", Value::BIGINT(nodata_count)}});

			value.Reinterpret(RasterTypes::STATS());
			return value;
		}
	};

	//! Calculate statistics of a band of a data cube.
	static void CalcStats(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw std::runtime_error("Band index cannot be negative");
			}
			const DataHeader header = arg_cube.GetHeader();
			if (band_index >= header.bands) {
				throw std::runtime_error("Band index out of range");
			}

			// Compute statistics for the specified band.
			CubeStats stats;
			auto stats_func = [&stats, &header, band_index](const CubeCellValue &v) {
				int32_t index = v.GetBandIndex(header);

				if (index == band_index) {
					stats.Update(v);
				}
			};
			DataCube::Apply(stats_func, arg_cube);

			// Set the result.
			result.SetValue(i, stats.ToValue());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Calculates statistics for a specific band (0-based index) of a datacube.

		The returned STRUCT contains: minimum, maximum, mean, standard deviation,
		count of valid (non-nodata) cells, and count of nodata cells.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CubeStats(databand, 0) AS stats FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		const ScalarFunction function("RT_CubeStats", {RasterTypes::DATACUBE(), LogicalType::INTEGER},
		                              RasterTypes::STATS(), CalcStats);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//! Aggregate version of RT_Stats, which computes the same statistics but across multiple datacubes.
struct RT_Stats_Agg {
	struct StatsAggState : RT_Stats::CubeStats {
		void Destroy() {
		}
	};

	struct StatsAggOp {
		template <class STATE>
		static void Initialize(STATE &state) {
			new (&state) STATE();
		}

		template <class STATE>
		static void Destroy(STATE &state, AggregateInputData &) {
			state.~STATE();
		}

		template <class STATE, class OP>
		static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
			// Merge two partial states (Statistics).
			target.Update(source);
		}

		template <class A_TYPE, class B_TYPE, class STATE, class OP>
		static void Operation(STATE &state, const A_TYPE &input, const B_TYPE &band_index,
		                      AggregateBinaryInput &agg_input) {
			// Process one input row:
			// A_TYPE = string_t (BLOB/DATACUBE),
			// B_TYPE = int32_t (INTEGER band index)
			Value blob = Value::BLOB(const_data_ptr_cast(input.GetData()), input.GetSize());

			DataCube arg_cube(agg_input.input.allocator.GetAllocator());
			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			const DataHeader header = arg_cube.GetHeader();
			const int32_t band_idx = static_cast<int32_t>(band_index);
			if (band_idx < 0) {
				throw std::runtime_error("Band index cannot be negative");
			}
			if (band_idx >= header.bands) {
				throw std::runtime_error("Band index out of range");
			}

			// Accumulate stats for all cells using Welford's online algorithm.
			auto stats_func = [&state, &header, band_index](const CubeCellValue &v) {
				int32_t index = v.GetBandIndex(header);

				if (index == band_index) {
					state.Update(v);
				}
			};
			DataCube::Apply(stats_func, arg_cube);
		}

		template <class INPUT_TYPE, class OPTS_TYPE, class STATE, class OP>
		static void ConstantOperation(STATE &state, const INPUT_TYPE &input, const OPTS_TYPE &opts,
		                              AggregateBinaryInput &agg_input, idx_t) {
			Operation<INPUT_TYPE, OPTS_TYPE, STATE, OP>(state, input, opts, agg_input);
		}

		template <class STATE>
		static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
			//! Produce the final STATS struct.
			finalize_data.result.SetValue(finalize_data.result_idx, state.ToValue());
		}

		static bool IgnoreNull() {
			return true;
		}
	};

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Calculates statistics for a specific band (0-based index) in a set of datacubes.

		The returned STRUCT contains: minimum, maximum, mean, standard deviation,
		count of valid (non-nodata) cells, and count of nodata cells.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CubeStats_Agg(databand_1, 0) FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "aggregate");

		AggregateFunction fun("RT_CubeStats_Agg", {RasterTypes::DATACUBE(), LogicalType::INTEGER}, RasterTypes::STATS(),
		                      AggregateFunction::StateSize<StatsAggState>,
		                      AggregateFunction::StateInitialize<StatsAggState, StatsAggOp>,
		                      AggregateFunction::BinaryScatterUpdate<StatsAggState, string_t, int32_t, StatsAggOp>,
		                      AggregateFunction::StateCombine<StatsAggState, StatsAggOp>,
		                      AggregateFunction::StateVoidFinalize<StatsAggState, StatsAggOp>,
		                      AggregateFunction::BinaryUpdate<StatsAggState, string_t, int32_t, StatsAggOp>);

		RegisterFunction<AggregateFunction>(loader, fun, CatalogType::AGGREGATE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                    tags);
	}
};

//======================================================================================================================
// RT_NullOrEmpty
//======================================================================================================================

struct RT_NullOrEmpty {
	//! Returns true if the data cube is null or empty, false otherwise.
	static void Apply(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

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

		const ScalarFunction function("RT_CubeNullOrEmpty", {RasterTypes::DATACUBE()}, LogicalType::BOOLEAN, Apply);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//======================================================================================================================
// RT_ChangeType
//======================================================================================================================

struct RT_ChangeType {
	//! Change the data type of a data cube.
	static void Apply(const LogicalType &logicalType, DataChunk &args, ExpressionState &state, Vector &result) {
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

		All arithmetic operations produce `DOUBLE` values internally. Use this function to convert the result
		to the desired storage type before writing to a raster file, or to reinterpret an existing band
		(e.g. from `INT16` to `FLOAT`).
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
	//! Apply a unary operation to a datacube.
	static void ApplyUnaryOp(CubeUnaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 1);
		const idx_t count = args.size();
		args.Flatten();

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

	//! Apply a binary operation to two datacubes.
	static void ApplyBinaryOp1(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

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

	//! Apply a binary operation to a datacube and a scalar value.
	static void ApplyBinaryOp2(CubeBinaryOp::Value op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

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
				RT_Math::ApplyUnaryOp(op, args, state, result);
			};
			ScalarFunction function =
			    ScalarFunction(function_name, {RasterTypes::DATACUBE()}, RasterTypes::DATACUBE(), executor);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, description, "",
			                                 tags);
		}

		// Register binary operations
		static constexpr std::array<std::tuple<const char *, CubeBinaryOp::Value, const char *>, 23> binary_ops = {{
		    // Arithmetic
		    {"RT_CubeAdd", CubeBinaryOp::ADD,
		     "Returns a data cube with each cell equal to the sum of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeSubtract", CubeBinaryOp::SUBTRACT,
		     "Returns a data cube with each cell equal to the left-hand cell minus the right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeMultiply", CubeBinaryOp::MULTIPLY,
		     "Returns a data cube with each cell equal to the product of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeDivide", CubeBinaryOp::DIVIDE,
		     "Returns a data cube with each cell equal to the left-hand cell divided by the right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubePow", CubeBinaryOp::POW,
		     "Returns a data cube with each cell raised to the power of the corresponding right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeMod", CubeBinaryOp::MOD,
		     "Returns a data cube with each cell equal to the remainder of dividing the left-hand cell by the "
		     "right-hand cell. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    // Comparison
		    {"RT_CubeEqual", CubeBinaryOp::EQUAL,
		     "Returns a data cube where each cell is 1 if the corresponding cells of the two inputs are equal, "
		     "0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeNotEqual", CubeBinaryOp::NOT_EQUAL,
		     "Returns a data cube where each cell is 1 if the corresponding cells of the two inputs differ, "
		     "0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeLess", CubeBinaryOp::LESS,
		     "Returns a data cube where each cell is 1 if the left-hand cell is strictly less than the right-hand "
		     "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		     "preserved."},
		    {"RT_CubeLessEqual", CubeBinaryOp::LESS_EQUAL,
		     "Returns a data cube where each cell is 1 if the left-hand cell is less than or equal to the right-hand "
		     "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		     "preserved."},
		    {"RT_CubeGreater", CubeBinaryOp::GREATER,
		     "Returns a data cube where each cell is 1 if the left-hand cell is strictly greater than the right-hand "
		     "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		     "preserved."},
		    {"RT_CubeGreaterEqual", CubeBinaryOp::GREATER_EQUAL,
		     "Returns a data cube where each cell is 1 if the left-hand cell is greater than or equal to the "
		     "right-hand "
		     "cell, 0 otherwise. Inputs can be two data cubes or a data cube and a scalar. No-data cells are "
		     "preserved."},
		    // Assignment / utility
		    {"RT_CubeSet", CubeBinaryOp::SET,
		     "Returns a data cube where valid cells are replaced by the corresponding values of a second data cube or "
		     "scalar. No-data cells in the source are preserved."},
		    {"RT_CubeSetNoData", CubeBinaryOp::SET_NODATA,
		     "Returns a data cube where no-data cells are replaced by the specified value. Valid cells in the source "
		     "are preserved."},
		    {"RT_CubeFill", CubeBinaryOp::FILL,
		     "Returns a data cube where all cells (including no-data) are unconditionally replaced by the "
		     "corresponding values of a second data cube or scalar, bypassing no-data checks."},
		    {"RT_CubeMin", CubeBinaryOp::MIN,
		     "Returns a data cube with each cell equal to the minimum of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"RT_CubeMax", CubeBinaryOp::MAX,
		     "Returns a data cube with each cell equal to the maximum of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    // Arithmetic operators
		    {"+", CubeBinaryOp::ADD,
		     "Returns a data cube with each cell equal to the sum of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"-", CubeBinaryOp::SUBTRACT,
		     "Returns a data cube with each cell equal to the left-hand cell minus the right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"*", CubeBinaryOp::MULTIPLY,
		     "Returns a data cube with each cell equal to the product of the corresponding cells of the two inputs. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"/", CubeBinaryOp::DIVIDE,
		     "Returns a data cube with each cell equal to the left-hand cell divided by the right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"^", CubeBinaryOp::POW,
		     "Returns a data cube with each cell raised to the power of the corresponding right-hand cell. "
		     "Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
		    {"%", CubeBinaryOp::MOD,
		     "Returns a data cube with each cell equal to the remainder of dividing the left-hand cell by the "
		     "right-hand cell. Inputs can be two data cubes or a data cube and a scalar. No-data cells are preserved."},
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
	RT_Stats::Register(loader);
	RT_Stats_Agg::Register(loader);
	RT_NullOrEmpty::Register(loader);
	RT_ChangeType::Register(loader);
	RT_Math::Register(loader);
}

} // namespace duckdb
