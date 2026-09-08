#include "raster_stats_functions.hpp"
#include "raster_types.hpp"
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
#include "modules/gdal_dataset_io.hpp"
#include "modules/geos_module.hpp"

namespace duckdb {

namespace {

//======================================================================================================================
// Utilities
//======================================================================================================================

//! Extracts file paths from a Value, which can be either a VARCHAR or a LIST of VARCHARs.
static std::vector<std::string> ExtractFilePaths(const Value &in_value) {
	std::vector<std::string> file_names;

	if (in_value.IsNull()) {
		throw InvalidInputException("Cannot extract file paths from a NULL value");
	}
	if (in_value.type().id() == LogicalTypeId::LIST) {
		const auto &children = ListValue::GetChildren(in_value);
		file_names.reserve(children.size());

		for (const auto &child : children) {
			if (child.type().id() != LogicalTypeId::VARCHAR) {
				throw InvalidInputException("Expected a VARCHAR value, but got " + child.type().ToString());
			}
			file_names.push_back(child.GetValue<std::string>());
		}
	} else {
		if (in_value.type().id() != LogicalTypeId::VARCHAR) {
			throw InvalidInputException("Expected a VARCHAR value, but got " + in_value.type().ToString());
		}
		file_names.push_back(in_value.GetValue<std::string>());
	}
	if (file_names.empty()) {
		throw InvalidInputException("No file paths provided");
	}
	return file_names;
}

//! Concatenates a vector of file paths into a single semicolon-separated string.
static std::string ConcatFilePaths(const std::vector<std::string> &file_names) {
	std::string result;

	for (size_t i = 0; i < file_names.size(); i++) {
		result += file_names[i];

		if (i < file_names.size() - 1) {
			result += ";";
		}
	}
	return result;
}

//! Load the data of a specific band from a GDAL dataset into a DataCube.
static RasterBounds LoadDataCubeBand(GDALDataset *dataset, const int32_t band_index, const GeometryExtent &bounds,
                                     DataCube &data_cube) {
	const int32_t raster_size_x = dataset->GetRasterXSize();
	const int32_t raster_size_y = dataset->GetRasterYSize();

	GDALRasterBand *band = dataset->GetRasterBand(band_index + 1);
	const GDALDataType data_type = band->GetRasterDataType();
	const int data_size = GDALGetDataTypeSizeBytes(data_type);

	int has_nodata = 0;
	double nodata = band->GetNoDataValue(&has_nodata);
	nodata = has_nodata ? nodata : NumericLimits<double>::Minimum();

	int32_t offset_x = 0;
	int32_t offset_y = 0;
	int32_t size_x = raster_size_x;
	int32_t size_y = raster_size_y;

	// Calculate the offset and size of the region of interest within the raster.
	if (bounds.HasXY()) {
		double x_min = bounds.x_min;
		double y_min = bounds.y_min;
		double x_max = bounds.x_max;
		double y_max = bounds.y_max;

		double gt[6] = {0};
		if (dataset->GetGeoTransform(gt) != CE_None) {
			gt[1] = 1.0;
			gt[5] = -1.0;
		}

		RasterCoord pt0 = RasterUtils::WorldCoordToRasterCoord(gt, x_min, y_min);
		RasterCoord pt1 = RasterUtils::WorldCoordToRasterCoord(gt, x_max, y_min);
		RasterCoord pt2 = RasterUtils::WorldCoordToRasterCoord(gt, x_max, y_max);
		RasterCoord pt3 = RasterUtils::WorldCoordToRasterCoord(gt, x_min, y_max);

		// Compute the bounding window of the region of interest.
		offset_x = MaxValue(0, MinValue(MinValue(pt0.col, pt1.col), MinValue(pt2.col, pt3.col)));
		offset_y = MaxValue(0, MinValue(MinValue(pt0.row, pt1.row), MinValue(pt2.row, pt3.row)));
		const int32_t max_col = MaxValue(MaxValue(pt0.col, pt1.col), MaxValue(pt2.col, pt3.col));
		const int32_t max_row = MaxValue(MaxValue(pt0.row, pt1.row), MaxValue(pt2.row, pt3.row));
		size_x = MaxValue(0, MinValue(raster_size_x, max_col + 1) - offset_x);
		size_y = MaxValue(0, MinValue(raster_size_y, max_row + 1) - offset_y);
	}

	// Prepare the data cube where the raster band data will be stored.

	DataHeader header = {DataFormat::Value::RAW, RasterUtils::GdalTypeToDataType(data_type), 1, size_x, size_y, nodata};
	data_cube.SetHeader(header, true);

	MemoryStream &data_buffer = data_cube.GetBuffer();
	const size_t cube_size = static_cast<size_t>(1) * size_x * size_y * data_size;
	data_buffer.GrowCapacity(cube_size);

	// Read the data of the desired band.

	data_ptr_t data_ptr = data_buffer.GetData() + sizeof(DataHeader);
	CPLErr read_err =
	    band->RasterIO(GF_Read, offset_x, offset_y, size_x, size_y, data_ptr, size_x, size_y, data_type, 0, 0, nullptr);

	if (read_err != CE_None) {
		const std::string error = RasterUtils::GetLastGdalErrorMsg();
		throw IOException("Failed to read the file: %s", error.c_str());
	}
	return RasterBounds(offset_x, offset_x + size_x, offset_y, offset_y + size_y);
}

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
		args.Flatten();

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
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		RasterTransformMatrix matrix;
		std::string matrix_str;

		GEOSLocalState &glocal_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSLocalState>();
		GEOSContextHandle_t geos_ctx = glocal_state.ctx;
		Point2D points[4];

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

			// Compute zonal statistics for the specified band.

			GEOSGeometry *raw_geom = GEOSLocalState::CreateGeometry(geos_ctx, args.data[5].GetValue(i));
			GEOSIntersectsGeometry wrap_geom(geos_ctx, raw_geom);

			CubeStats stats;
			auto stats_func = [&](const CubeCellValue &v) {
				RasterCoord coord = v.GetCoord(header);

				int32_t tx = tile_x * blocksize_x + coord.col;
				int32_t ty = tile_y * blocksize_y + coord.row;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);

				if (wrap_geom.Intersects(points)) {
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

		static void ScatterUpdateGeom(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
		                              Vector &input_states, idx_t count) {
			D_ASSERT(input_count == 6);

			// Arguments:
			//	ARG1_TYPE = string_t (BLOB/DATACUBE blob)
			//	ARG2_TYPE = int32_t  (band)
			//	ARG3_TYPE = int32_t  (tile_x)
			//	ARG4_TYPE = int32_t  (tile_y)
			//	ARG5_TYPE = JSON	 (metadata)
			//	ARG6_TYPE = GEOMETRY (geometry)

			UnifiedVectorFormat state_data;
			input_states.ToUnifiedFormat(count, state_data);

			UnifiedVectorFormat input_data[6];
			for (idx_t j = 0; j < 6; j++) {
				inputs[j].ToUnifiedFormat(count, input_data[j]);
			}

			auto states = UnifiedVectorFormat::GetData<data_ptr_t>(state_data);
			auto param0 = UnifiedVectorFormat::GetData<string_t>(input_data[0]);
			auto param1 = UnifiedVectorFormat::GetData<int32_t>(input_data[1]);
			auto param2 = UnifiedVectorFormat::GetData<int32_t>(input_data[2]);
			auto param3 = UnifiedVectorFormat::GetData<int32_t>(input_data[3]);
			auto param4 = UnifiedVectorFormat::GetData<string_t>(input_data[4]);
			auto param5 = UnifiedVectorFormat::GetData<string_t>(input_data[5]);

			DataCube arg_cube(aggr_input_data.allocator.GetAllocator());

			RasterTransformMatrix matrix;
			std::string matrix_str;

			GEOSLocalState glocal_state;
			GEOSContextHandle_t geos_ctx = glocal_state.ctx;
			Point2D points[4];

			for (idx_t i = 0; i < count; i++) {
				auto state_idx = state_data.sel->get_index(i);

				// Check if we must skip this row.

				bool row_valid = state_data.validity.RowIsValid(state_idx);
				if (!row_valid) {
					continue;
				}
				for (idx_t j = 0; j < 6; j++) {
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
				const int32_t tile_x = param2[input_data[2].sel->get_index(i)];
				const int32_t tile_y = param3[input_data[3].sel->get_index(i)];
				const string_t &metadata = param4[input_data[4].sel->get_index(i)];
				const string_t &geometry = param5[input_data[5].sel->get_index(i)];

				arg_cube.LoadBlob(const_data_ptr_cast(blob.GetData()), blob.GetSize());
				arg_cube.EnsureRaw();

				// Validate the input parameters.

				if (band_index < 0) {
					throw InvalidInputException("Band index cannot be negative");
				}

				if (tile_x < 0) {
					throw InvalidInputException("Tile X coordinate cannot be negative");
				}

				if (tile_y < 0) {
					throw InvalidInputException("Tile Y coordinate cannot be negative");
				}

				const DataHeader header = arg_cube.GetHeader();

				if (band_index >= header.bands) {
					throw InvalidInputException("Band index out of range: %d >= %d", band_index, header.bands);
				}

				std::string metadata_s = metadata.GetString();
				if (metadata_s != matrix_str) {
					matrix = RasterUtils::GetTransformMatrix(metadata_s);
					matrix_str = metadata_s;
				}

				const double(&gt)[6] = matrix.affine;
				const int32_t &blocksize_x = matrix.blocksize_x;
				const int32_t &blocksize_y = matrix.blocksize_y;

				// Compute statistics for the specified band and update the state.

				GEOSGeometry *raw_geom = GEOSLocalState::CreateGeometry(geos_ctx, geometry);
				GEOSIntersectsGeometry wrap_geom(geos_ctx, raw_geom);

				auto stats_func = [&](const CubeCellValue &v) {
					RasterCoord coord = v.GetCoord(header);

					int32_t tx = tile_x * blocksize_x + coord.col;
					int32_t ty = tile_y * blocksize_y + coord.row;
					points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
					points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
					points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
					points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);

					if (wrap_geom.Intersects(points)) {
						state.stats.Update(v);
					}
				};
				DataCube::Apply(stats_func, arg_cube, band_index);
			}
		}

		static void SimpleUpdateGeom(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count,
		                             data_ptr_t state_p, idx_t count) {
			Vector states(Value::POINTER(CastPointerToValue(state_p)));
			ScatterUpdateGeom(inputs, aggr_input_data, input_count, states, count);
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
		SELECT RT_CubeStats_Agg(databand_1, 0) AS stats FROM RT_Read('some/file/path/filename.tif');
		SELECT RT_CubeStats_Agg(databand_1, 0, tile_x, tile_y, metadata, geometry) AS stats FROM RT_Read('some/file/path/filename.tif');
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "aggregate");

		AggregateFunctionSet function_set("RT_CubeStats_Agg");

		const AggregateFunction func01(
		    "RT_CubeStats_Agg", {RasterTypes::DATACUBE(), LogicalType::INTEGER}, RasterTypes::STATS(),
		    AggregateFunction::StateSize<FunctionAggState>,
		    AggregateFunction::StateInitialize<FunctionAggState, FunctionAggOp>, FunctionAggOp::ScatterUpdate,
		    AggregateFunction::StateCombine<FunctionAggState, FunctionAggOp>,
		    AggregateFunction::StateVoidFinalize<FunctionAggState, FunctionAggOp>, FunctionAggOp::SimpleUpdate);

		function_set.AddFunction(func01);

		const AggregateFunction func02(
		    "RT_CubeStats_Agg",
		    {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::INTEGER,
		     LogicalType::JSON(), LogicalType::GEOMETRY()},
		    RasterTypes::STATS(), AggregateFunction::StateSize<FunctionAggState>,
		    AggregateFunction::StateInitialize<FunctionAggState, FunctionAggOp>, FunctionAggOp::ScatterUpdateGeom,
		    AggregateFunction::StateCombine<FunctionAggState, FunctionAggOp>,
		    AggregateFunction::StateVoidFinalize<FunctionAggState, FunctionAggOp>, FunctionAggOp::SimpleUpdateGeom);

		function_set.AddFunction(func02);

		RegisterFunction<AggregateFunctionSet>(loader, function_set, CatalogType::AGGREGATE_FUNCTION_ENTRY, DESCRIPTION,
		                                       EXAMPLE, tags);
	}
};

//======================================================================================================================
// RT_RasterStats
//======================================================================================================================

struct RT_RasterStats {
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

	//! Calculate statistics of a band in a raster.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 2);
		const idx_t count = args.size();
		args.Flatten();

		DataCube data_cube(Allocator::Get(state.GetContext()));

		auto &client_context = state.GetContext();
		GDALDatasetUniquePtr dataset;
		std::string dataset_path;

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			const std::vector<std::string> file_paths = ExtractFilePaths(args.data[0].GetValue(i));
			const std::string input_path = ConcatFilePaths(file_paths);

			// Validate the input parameters.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			// Open the dataset if the file path has changed.

			if (dataset_path != input_path) {
				dataset = GDALDatasetUniquePtr(DuckDBDatasetFactory::OpenDataset(client_context, file_paths, {}));
				dataset_path = input_path;
			}

			if (band_index >= dataset->GetRasterCount()) {
				throw InvalidInputException("Band index out of range");
			}

			LoadDataCubeBand(dataset.get(), band_index, GeometryExtent::Unknown(), data_cube);

			// Compute statistics for the specified band.

			RT_Stats::CubeStats stats;
			auto stats_func = [&stats](const CubeCellValue &v) {
				stats.Update(v);
			};
			DataCube::Apply(stats_func, data_cube, 0);

			// Set the result.
			result.SetValue(i, stats.ToValue());
		}
	}

	//! Calculate statistics of a band in a raster for those valid (non-nodata) cells that fall within a geometry.
	static void ExecuteGeom(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 3);
		const idx_t count = args.size();
		args.Flatten();

		DataCube data_cube(Allocator::Get(state.GetContext()));

		auto &client_context = state.GetContext();
		GDALDatasetUniquePtr dataset;
		std::string dataset_path;

		GEOSLocalState &glocal_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSLocalState>();
		GEOSContextHandle_t geos_ctx = glocal_state.ctx;
		Point2D points[4];

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			const std::vector<std::string> file_paths = ExtractFilePaths(args.data[0].GetValue(i));
			const std::string input_path = ConcatFilePaths(file_paths);

			// Validate the input parameters.

			const int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
			}

			// Open the dataset if the file path has changed.

			if (dataset_path != input_path) {
				dataset = GDALDatasetUniquePtr(DuckDBDatasetFactory::OpenDataset(client_context, file_paths, {}));
				dataset_path = input_path;
			}

			if (band_index >= dataset->GetRasterCount()) {
				throw InvalidInputException("Band index out of range");
			}

			GEOSGeometry *raw_geom = GEOSLocalState::CreateGeometry(geos_ctx, args.data[2].GetValue(i));
			GEOSIntersectsGeometry wrap_geom(geos_ctx, raw_geom);
			GeometryExtent bbox_geom = GEOSLocalState::GetGeometryExtent(geos_ctx, raw_geom);

			RasterBounds window = LoadDataCubeBand(dataset.get(), band_index, bbox_geom, data_cube);
			const DataHeader header = data_cube.GetHeader();

			// Compute zonal statistics for the specified band.

			double gt[6] = {0};
			if (dataset->GetGeoTransform(gt) != CE_None) {
				gt[1] = 1.0;
				gt[5] = -1.0;
			}

			// Shift the origin so pixel (0,0) of the cropped window maps to its real world position.
			Point2D origin = RasterUtils::RasterCoordToWorldCoord(gt, window.min_col, window.min_row);
			gt[0] = origin.x;
			gt[3] = origin.y;

			RT_Stats::CubeStats stats;
			auto stats_func = [&](const CubeCellValue &v) {
				RasterCoord coord = v.GetCoord(header);

				int32_t tx = coord.col;
				int32_t ty = coord.row;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);

				if (wrap_geom.Intersects(points)) {
					stats.Update(v);
				}
			};
			DataCube::Apply(stats_func, data_cube, 0);

			// Set the result.
			result.SetValue(i, stats.ToValue());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Calculates statistics for a specific band (0-based index) of a raster.

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

		Just to compute statistics for a specific band of a raster:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| [`filepath`, `filepaths`] | [VARCHAR, VARCHAR[]] | The file path[s] of the raster[s] to compute statistics for. |
		| `band` | INTEGER | The 0-based index of the band to compute statistics for. |

		To compute statistics for a specific band of a raster, but only for those valid (non-nodata)
		cells that fall within a geometry (Zonal statistics):

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| [`filepath`, `filepaths`] | [VARCHAR, VARCHAR[]] | The file path[s] of the raster[s] to compute statistics for. |
		| `band` | INTEGER | The 0-based index of the band to compute statistics for. |
		| `geometry` | GEOMETRY | The geometry to use for spatial filtering. |
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT RT_Stats('some/file/path/filename.tif', 0);
		SELECT RT_Stats(['some/file/path/filename_1.tif', 'some/file/path/filename_2.tif'], 0);
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "scalar");

		ScalarFunctionSet function_set("RT_Stats");

		ScalarFunction function_01 =
		    ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, RasterTypes::STATS(), Execute);

		function_01.SetVolatile();
		function_set.AddFunction(function_01);

		ScalarFunction function_02 = ScalarFunction({LogicalType::LIST(LogicalType::VARCHAR), LogicalType::INTEGER},
		                                            RasterTypes::STATS(), Execute);

		function_02.SetVolatile();
		function_set.AddFunction(function_02);

		ScalarFunction function_03 =
		    ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::GEOMETRY()}, RasterTypes::STATS(),
		                   ExecuteGeom, nullptr, nullptr, nullptr, InitLocal);

		function_03.SetVolatile();
		function_set.AddFunction(function_03);

		ScalarFunction function_04 =
		    ScalarFunction({LogicalType::LIST(LogicalType::VARCHAR), LogicalType::INTEGER, LogicalType::GEOMETRY()},
		                   RasterTypes::STATS(), ExecuteGeom, nullptr, nullptr, nullptr, InitLocal);

		function_04.SetVolatile();
		function_set.AddFunction(function_04);

		RegisterFunction<ScalarFunctionSet>(loader, function_set, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION,
		                                    EXAMPLE, tags);
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
	RT_RasterStats::Register(loader);
}

} // namespace duckdb
