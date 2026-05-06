#include "raster_stats_functions.hpp"
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

//======================================================================================================================
// RT_Stats_Agg
//======================================================================================================================

struct RT_Stats_Agg {
	struct StatsAggState : RT_Stats::CubeStats {
		void Destroy() {
		}
	};

	//! Aggregate version of RT_Stats, which computes the same statistics but across multiple datacubes.
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

} // namespace

// #####################################################################################################################
// Register Stats Functions
// #####################################################################################################################

void RasterStatsFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Stats::Register(loader);
	RT_Stats_Agg::Register(loader);
}

} // namespace duckdb
