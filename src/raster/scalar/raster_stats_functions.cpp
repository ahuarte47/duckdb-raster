#include "raster_stats_functions.hpp"
#include "raster_utils.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/function/scalar_function.hpp"

// GEOS
#include "geos_c.h"
#include "modules/geos_state.hpp"

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

	//------------------------------------------------------------------------------------------------------------------
	// Init Local (Only for the ExecuteGeom function)
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<FunctionLocalState> InitLocal(ExpressionState &state, const BoundFunctionExpression &expr,
	                                                FunctionData *bind_data) {
		return make_uniq<GEOSLocalState>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Calculate statistics of a band in a data cube.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			// Validate the input parameters.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			const DataHeader header = arg_cube.GetHeader();

			if (band_index >= header.bands) {
				throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
			}

			// Compute statistics for the specified band.

			CubeStats stats;
			auto stats_func = [&stats](const CubeCellValue &v) {
				stats.Update(v);
			};
			DataCube::Apply(stats_func, arg_cube, band_index);

			// Set the result.
			result.SetValue(i, stats.ToValue());
		}
	}

	//! Calculate statistics of a band in a data cube for those valid (non-nodata) cells that fall within a geometry.
	static void ExecuteGeom(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		RasterTransformMatrix matrix;
		std::string matrix_str;

		// Functions to hold the spatial coordinates of cells for the current row.

		GEOSLocalState &glocal_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSLocalState>();
		GEOSContextHandle_t geos_ctx = glocal_state.ctx;

		auto geometry_free = [geos_ctx](GEOSGeometry *g) {
			if (g) {
				GEOSGeom_destroy_r(geos_ctx, g);
			}
		};
		auto prep_geom_free = [geos_ctx](const GEOSPreparedGeometry *g) {
			if (g) {
				GEOSPreparedGeom_destroy_r(geos_ctx, g);
			}
		};
		auto extract_geometry =
		    [&geos_ctx](Value arg_value, std::unique_ptr<GEOSGeometry, decltype(geometry_free)> &raw_geom,
		                std::unique_ptr<const GEOSPreparedGeometry, decltype(prep_geom_free)> &prep_geom,
		                GeometryExtent &extent_geom) {
			    raw_geom.reset(GEOSLocalState::CreateGeometry(geos_ctx, arg_value));
			    if (!raw_geom) {
				    throw InvalidInputException("Failed to create geometry from input value");
			    }
			    prep_geom.reset(GEOSPrepare_r(geos_ctx, raw_geom.get()));
			    if (!prep_geom) {
				    throw InvalidInputException("Failed to prepare input geometry");
			    }
			    extent_geom = GEOSLocalState::GetGeometryExtent(geos_ctx, raw_geom.get());
		    };

		std::unique_ptr<GEOSGeometry, decltype(geometry_free)> raw_geom(nullptr, geometry_free);
		std::unique_ptr<const GEOSPreparedGeometry, decltype(prep_geom_free)> prep_geom(nullptr, prep_geom_free);
		GeometryExtent extent_geom;
		bool geometry_is_constant = false;
		Point2D points[4];

		if (count > 0 && args.data[5].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			extract_geometry(args.data[5].GetValue(0), raw_geom, prep_geom, extent_geom);
			geometry_is_constant = true;
		}

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.

		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			// Validate the input parameters.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			const DataHeader header = arg_cube.GetHeader();

			if (band_index >= header.bands) {
				throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
			}

			int32_t tile_x = args.data[2].GetValue(i).GetValue<int32_t>();
			if (tile_x < 0) {
				throw InvalidInputException("Tile X coordinate cannot be negative");
			}

			int32_t tile_y = args.data[3].GetValue(i).GetValue<int32_t>();
			if (tile_y < 0) {
				throw InvalidInputException("Tile Y coordinate cannot be negative");
			}

			std::string metadata = args.data[4].GetValue(i).GetValue<string>();
			if (metadata != matrix_str) {
				matrix = RasterUtils::GetTransformMatrix(metadata);
				matrix_str = metadata;
			}

			const double(&gt)[6] = matrix.affine;
			const int32_t &blocksize_x = matrix.blocksize_x;
			const int32_t &blocksize_y = matrix.blocksize_y;

			// Parse input geometry?
			if (!geometry_is_constant) {
				extract_geometry(args.data[5].GetValue(i), raw_geom, prep_geom, extent_geom);
			}
			if (!prep_geom) {
				throw InvalidInputException("Failed to prepare geometry for row %lu", i);
			}

			// Compute zonal statistics for the specified band.

			auto coord_intersects_geometry = [&](const RasterCoord &coord) {
				int32_t tx = tile_x * blocksize_x + coord.col;
				int32_t ty = tile_y * blocksize_y + coord.row;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);

				if (!GEOSLocalState::Intersects(extent_geom, points)) {
					return false;
				}
				return GEOSLocalState::Intersects(geos_ctx, prep_geom.get(), points);
			};

			CubeStats stats;
			auto stats_func = [&](const CubeCellValue &v) {
				RasterCoord coord = v.GetCoord(header);

				if (coord_intersects_geometry(coord)) {
					stats.Update(v);
				}
			};
			DataCube::Apply(stats_func, arg_cube, band_index);

			// Set the result.
			result.SetValue(i, stats.ToValue());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Calculates statistics for a specific band (0-based index) of a datacube.

		The returned value is a `STRUCT` with the following fields:

		| Field | Type | Description |
		| ----- | ---- | ----------- |
		| `minimum` | DOUBLE | Minimum pixel value among valid (non-nodata) cells. |
		| `maximum` | DOUBLE | Maximum pixel value among valid (non-nodata) cells. |
		| `sum` | DOUBLE | Sum of all valid pixel values. |
		| `mean` | DOUBLE | Mean (average) of all valid pixel values. |
		| `stddev` | DOUBLE | Population standard deviation of all valid pixel values. |
		| `valid_count` | BIGINT | Number of valid (non-nodata) cells. |
		| `nodata_count` | BIGINT | Number of nodata cells. |

		Function accepts two different forms with the following parameters.

		Just to compute statistics for a specific band of a datacube:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The datacube column to compute statistics for. |
		| `band` | INTEGER | The 0-based index of the band to compute statistics for. |

		To compute statistics for a specific band of a datacube, but only for those valid (non-nodata)
		cells that fall within a geometry (Zonal statistics):

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The datacube column to compute statistics for. |
		| `band` | INTEGER | The 0-based index of the band to compute statistics for. |
		| `tile_x` | INTEGER | The tile x coordinate of the tile. |
		| `tile_y` | INTEGER | The tile y coordinate of the tile. |
		| `metadata` | JSON | Raster metadata providing the affine geotransform matrix and tile block size. |
		| `geometry` | GEOMETRY | The geometry to use for spatial filtering. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CubeStats(databand, 0) AS stats FROM RT_Read('some/file/path/filename.tif');
		SELECT RT_CubeStats(databand, 0, tile_x, tile_y, metadata, geometry) AS stats FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		ScalarFunctionSet function_set("RT_CubeStats");

		const ScalarFunction func01 =
		    ScalarFunction({RasterTypes::DATACUBE(), LogicalType::INTEGER}, RasterTypes::STATS(), Execute);

		function_set.AddFunction(func01);

		const ScalarFunction func02 =
		    ScalarFunction({RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
		                    LogicalType::JSON(), LogicalType::GEOMETRY()},
		                   RasterTypes::STATS(), ExecuteGeom, nullptr, nullptr, nullptr, InitLocal);

		function_set.AddFunction(func02);

		RegisterFunction<ScalarFunctionSet>(loader, function_set, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION,
		                                    EXAMPLE, tags);
	}
};

//======================================================================================================================
// RT_Stats_Agg
//======================================================================================================================

struct RT_Stats_Agg {
	//! State for the aggregate function.
	struct FunctionAggState {
		RT_Stats::CubeStats stats;
		void Destroy() {
		}
	};

	//! Aggregate version of RT_CubeStats, which computes the statistics but across multiple datacubes.
	struct FunctionAggOp {
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
			// Merge two partial states.
			target.stats.Update(source.stats);
		}

		static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
		                          Vector &input_states, idx_t count) {
			D_ASSERT(input_count == 2);

			// Arguments:
			//	ARG1_TYPE = string_t (BLOB/DATACUBE blob)
			//	ARG2_TYPE = int32_t  (band)

			UnifiedVectorFormat state_data;
			input_states.ToUnifiedFormat(count, state_data);

			UnifiedVectorFormat input_data[2];
			for (idx_t j = 0; j < 2; j++) {
				inputs[j].ToUnifiedFormat(count, input_data[j]);
			}

			auto states = UnifiedVectorFormat::GetData<data_ptr_t>(state_data);
			auto param0 = UnifiedVectorFormat::GetData<string_t>(input_data[0]);
			auto param1 = UnifiedVectorFormat::GetData<int32_t>(input_data[1]);

			DataCube arg_cube(aggr_input_data.allocator.GetAllocator());

			for (idx_t i = 0; i < count; i++) {
				auto state_idx = state_data.sel->get_index(i);

				// Check if we must skip this row.

				bool row_valid = state_data.validity.RowIsValid(state_idx);
				if (!row_valid) {
					continue;
				}
				for (idx_t j = 0; j < 2; j++) {
					auto input_idx = input_data[j].sel->get_index(i);

					if (!input_data[j].validity.RowIsValid(input_idx)) {
						row_valid = false;
						break;
					}
				}
				if (!row_valid) {
					continue;
				}

				// Get the input parameters for this row.

				auto &state = *reinterpret_cast<FunctionAggState *>(states[state_idx]);
				const string_t &blob = param0[input_data[0].sel->get_index(i)];
				const int32_t band_index = param1[input_data[1].sel->get_index(i)];

				arg_cube.LoadBlob(const_data_ptr_cast(blob.GetData()), blob.GetSize());
				arg_cube.EnsureRaw();

				// Validate the input parameters.

				if (band_index < 0) {
					throw InvalidInputException("Band index cannot be negative");
				}

				const DataHeader header = arg_cube.GetHeader();

				if (band_index >= header.bands) {
					throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
				}

				// Compute statistics for the specified band and update the state.

				auto stats_func = [&state](const CubeCellValue &v) {
					state.stats.Update(v);
				};
				DataCube::Apply(stats_func, arg_cube, band_index);
			}
		}

		static void SimpleUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
		                         data_ptr_t state_p, idx_t count) {
			Vector states(Value::POINTER(CastPointerToValue(state_p)));
			ScatterUpdate(inputs, aggr_input_data, input_count, states, count);
		}

		template <class STATE>
		static void Finalize(STATE &state, AggregateFinalizeData &finalize_data) {
			//! Produce the final result.
			auto r = state.stats.ToValue();
			finalize_data.result.SetValue(finalize_data.result_idx, r);
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

		The returned value is a `STRUCT` with the following fields:

		| Field | Type | Description |
		| ----- | ---- | ----------- |
		| `minimum` | DOUBLE | Minimum pixel value among valid (non-nodata) cells. |
		| `maximum` | DOUBLE | Maximum pixel value among valid (non-nodata) cells. |
		| `sum` | DOUBLE | Sum of all valid pixel values. |
		| `mean` | DOUBLE | Mean (average) of all valid pixel values. |
		| `stddev` | DOUBLE | Population standard deviation of all valid pixel values. |
		| `valid_count` | BIGINT | Number of valid (non-nodata) cells. |
		| `nodata_count` | BIGINT | Number of nodata cells. |

		Function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The datacube column to compute statistics for. |
		| `band` | INTEGER | The 0-based index of the band to compute statistics for. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CubeStats_Agg(databand_1, 0) AS stats FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "aggregate");

		AggregateFunction fun(
		    "RT_CubeStats_Agg", {RasterTypes::DATACUBE(), LogicalType::INTEGER}, RasterTypes::STATS(),
		    AggregateFunction::StateSize<FunctionAggState>,
		    AggregateFunction::StateInitialize<FunctionAggState, FunctionAggOp>, FunctionAggOp::ScatterUpdate,
		    AggregateFunction::StateCombine<FunctionAggState, FunctionAggOp>,
		    AggregateFunction::StateVoidFinalize<FunctionAggState, FunctionAggOp>, FunctionAggOp::SimpleUpdate);

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
