#include "raster_values_functions.hpp"
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
// RT_RasterValue
//======================================================================================================================

struct RT_RasterValue {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Returns the value in a datacube at the specified pixel coordinates (band, col, row).
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 5);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually to share the same DataCube instance.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			const double default_value = args.data[4].GetValue(i).GetValue<double>();

			// Validate the input pixel coordinates.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			const int32_t col = args.data[2].GetValue(i).GetValue<int32_t>();
			if (col < 0) {
				result.SetValue(i, Value::DOUBLE(default_value));
				continue;
			}

			const int32_t row = args.data[3].GetValue(i).GetValue<int32_t>();
			if (row < 0) {
				result.SetValue(i, Value::DOUBLE(default_value));
				continue;
			}

			DataHeader header = DataCube::ReadHeader(blob);

			if (band_index >= header.bands) {
				throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
			}
			if (col >= header.cols) {
				result.SetValue(i, Value::DOUBLE(default_value));
				continue;
			}
			if (row >= header.rows) {
				result.SetValue(i, Value::DOUBLE(default_value));
				continue;
			}

			// Extract the value at the specified coordinates and return it.

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			double value = arg_cube.GetValue<double>(band_index, col, row);
			result.SetValue(i, Value::DOUBLE(value));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the value in a datacube at the specified pixel coordinates (band, col, row).

		The function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The input datacube column. |
		| `band` | INTEGER | The 0-based index of the band to read the value from. |
		| `col` | INTEGER | The pixel column index within the tile. |
		| `row` | INTEGER | The pixel row index within the tile. |
		| `default_value` | DOUBLE | The value to return if the specified coordinates are out of bounds. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_RasterValue(databand_1, 0, 10, 20, -9999.0) FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		const ScalarFunction function("RT_RasterValue",
		                              {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
		                               LogicalType::INTEGER, LogicalType::DOUBLE},
		                              LogicalType::DOUBLE, Execute);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//======================================================================================================================
// RT_RasterValue_Agg
//======================================================================================================================

struct RT_RasterValue_Agg {
	//! State for the aggregate function.
	struct FunctionAggState {
		bool need_compute = true;
		double value = 0.0;
		void Destroy() {
		}
	};

	//! Aggregate version of RT_Stats, which computes the statistics but across multiple datacubes.
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
			if (!source.need_compute) {
				target.need_compute = false;
				target.value = source.value;
			}
		}

		static void ScatterUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
		                          Vector &input_states, idx_t count) {
			D_ASSERT(input_count == 8);

			// Arguments:
			//	ARG1_TYPE = string_t (BLOB/DATACUBE blob)
			//	ARG2_TYPE = int32_t  (band)
			//	ARG3_TYPE = int32_t  (col)
			//	ARG4_TYPE = int32_t  (row)
			//	ARG5_TYPE = int32_t  (tile_x)
			//	ARG6_TYPE = int32_t  (tile_y)
			//	ARG7_TYPE = string_t (JSON metadata)
			//	ARG8_TYPE = double   (default value)

			UnifiedVectorFormat state_data;
			input_states.ToUnifiedFormat(count, state_data);

			UnifiedVectorFormat input_data[8];
			for (idx_t j = 0; j < 8; j++) {
				inputs[j].ToUnifiedFormat(count, input_data[j]);
			}

			auto states = UnifiedVectorFormat::GetData<data_ptr_t>(state_data);
			auto param0 = UnifiedVectorFormat::GetData<string_t>(input_data[0]);
			auto param1 = UnifiedVectorFormat::GetData<int32_t>(input_data[1]);
			auto param2 = UnifiedVectorFormat::GetData<int32_t>(input_data[2]);
			auto param3 = UnifiedVectorFormat::GetData<int32_t>(input_data[3]);
			auto param4 = UnifiedVectorFormat::GetData<int32_t>(input_data[4]);
			auto param5 = UnifiedVectorFormat::GetData<int32_t>(input_data[5]);
			auto param6 = UnifiedVectorFormat::GetData<string_t>(input_data[6]);
			auto param7 = UnifiedVectorFormat::GetData<double>(input_data[7]);

			DataCube arg_cube(aggr_input_data.allocator.GetAllocator());

			RasterTransformMatrix matrix;
			std::string matrix_str;

			for (idx_t i = 0; i < count; i++) {
				auto state_idx = state_data.sel->get_index(i);

				// Check if we must skip this row.

				bool row_valid = state_data.validity.RowIsValid(state_idx);
				if (!row_valid) {
					continue;
				}
				for (idx_t j = 0; j < 8; j++) {
					auto input_idx = input_data[j].sel->get_index(i);

					if (!input_data[j].validity.RowIsValid(input_idx)) {
						row_valid = false;
						break;
					}
				}
				if (!row_valid) {
					continue;
				}

				auto &state = *reinterpret_cast<FunctionAggState *>(states[state_idx]);
				if (!state.need_compute) {
					continue;
				}

				// Get the input parameters for this row.

				const string_t &blob = param0[input_data[0].sel->get_index(i)];
				const int32_t band_index = param1[input_data[1].sel->get_index(i)];
				const int32_t col = param2[input_data[2].sel->get_index(i)];
				const int32_t row = param3[input_data[3].sel->get_index(i)];
				const int32_t tile_x = param4[input_data[4].sel->get_index(i)];
				const int32_t tile_y = param5[input_data[5].sel->get_index(i)];
				const string_t &metadata = param6[input_data[6].sel->get_index(i)];
				const double default_value = param7[input_data[7].sel->get_index(i)];

				state.value = default_value;

				// Validate the input parameters.

				if (band_index < 0) {
					throw InvalidInputException("Band index cannot be negative");
				}

				const std::string metadata_str = metadata.GetString();
				if (metadata_str != matrix_str) {
					matrix = RasterUtils::GetTransformMatrix(metadata_str);
					matrix_str = metadata_str;
				}

				const int32_t tx = col - tile_x * matrix.blocksize_x;
				const int32_t ty = row - tile_y * matrix.blocksize_y;

				if (tx < 0 || ty < 0) {
					continue;
				}

				const DataHeader header = DataCube::ReadHeader(const_data_ptr_cast(blob.GetData()), blob.GetSize());

				if (band_index >= header.bands) {
					throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
				}
				if (tx >= header.cols || ty >= header.rows) {
					continue;
				}

				// Extract the value at the specified coordinates and save it.

				arg_cube.LoadBlob(const_data_ptr_cast(blob.GetData()), blob.GetSize());
				arg_cube.EnsureRaw();

				state.value = arg_cube.GetValue<double>(band_index, tx, ty);
				state.need_compute = false;
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
			auto r = state.value;
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
		Returns the value in a specified band of a datacube at the specified pixel coordinates (column, row).

		The function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The input datacube column. |
		| `band` | INTEGER | The 0-based index of the band to read the value from. |
		| `col` | INTEGER | The pixel column index within the tile. |
		| `row` | INTEGER | The pixel row index within the tile. |
		| `default_value` | DOUBLE | The value to return if the specified coordinates are out of bounds. |
    )";

	static constexpr auto EXAMPLE = R"(
		SELECT
			RT_RasterValue_Agg(databand_1, 0, 0, 0, tile_x, tile_y, metadata, -9999.0)
		FROM
			RT_Read('some/file/path/filename.tif')
		;
    )";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "aggregate");

		AggregateFunction fun(
		    "RT_RasterValue_Agg",
		    {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
		     LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::JSON(), LogicalType::DOUBLE},
		    LogicalType::DOUBLE, AggregateFunction::StateSize<FunctionAggState>,
		    AggregateFunction::StateInitialize<FunctionAggState, FunctionAggOp>, FunctionAggOp::ScatterUpdate,
		    AggregateFunction::StateCombine<FunctionAggState, FunctionAggOp>,
		    AggregateFunction::StateVoidFinalize<FunctionAggState, FunctionAggOp>, FunctionAggOp::SimpleUpdate);

		RegisterFunction<AggregateFunction>(loader, fun, CatalogType::AGGREGATE_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                    tags);
	}
};

} // namespace

//======================================================================================================================
// RT_CoordValue
//======================================================================================================================

struct RT_CoordValue {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Returns the value in a datacube at the specified world coordinates (band, x, y).
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		RasterTransformMatrix matrix;
		std::string matrix_str;

		// We loop over rows manually to share the same DataCube instance.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);
			const double default_value = args.data[5].GetValue(i).GetValue<double>();

			// Validate the input parameters.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			const double x = args.data[2].GetValue(i).GetValue<double>();
			const double y = args.data[3].GetValue(i).GetValue<double>();

			DataHeader header = DataCube::ReadHeader(blob);

			if (band_index >= header.bands) {
				throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
			}

			std::string metadata = args.data[4].GetValue(i).GetValue<string>();
			if (metadata != matrix_str) {
				matrix = RasterUtils::GetTransformMatrix(metadata);
				matrix_str = metadata;
			}

			// Extract the value at the specified coordinates and return it.

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			RasterCoord coord = RasterUtils::WorldCoordToRasterCoord(matrix.affine, x, y);
			if (coord.col < 0 || coord.row < 0 || coord.col >= header.cols || coord.row >= header.rows) {
				result.SetValue(i, Value::DOUBLE(default_value));
				continue;
			}

			double value = arg_cube.GetValue<double>(band_index, coord.col, coord.row);
			result.SetValue(i, Value::DOUBLE(value));
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Returns the value in a datacube at the specified world coordinates (band, x, y).

		The function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The input datacube column. |
		| `band` | INTEGER | The 0-based index of the band to read the value from. |
		| `x` | DOUBLE | The x-coordinate of the pixel within the tile. |
		| `y` | DOUBLE | The y-coordinate of the pixel within the tile. |
		| `metadata` | JSON | The metadata associated with the datacube. |
		| `default_value` | DOUBLE | The value to return if the specified coordinates are out of bounds. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_CoordValue(databand_1, 0, -1.28, 42.25, metadata, -9999.0) FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		const ScalarFunction function("RT_CoordValue",
		                              {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::DOUBLE,
		                               LogicalType::DOUBLE, LogicalType::JSON(), LogicalType::DOUBLE},
		                              LogicalType::DOUBLE, Execute);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

// #####################################################################################################################
// Register Values Functions
// #####################################################################################################################

void RasterValuesFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_RasterValue::Register(loader);
	RT_RasterValue_Agg::Register(loader);
	RT_CoordValue::Register(loader);
}

} // namespace duckdb
