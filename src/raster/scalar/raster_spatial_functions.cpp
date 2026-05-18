#include "raster_spatial_functions.hpp"
#include "raster_utils.hpp"
#include "data_cube.hpp"
#include "function_builder.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/function_entry.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

// GEOS
#include "geos_c.h"

namespace duckdb {

namespace {

//======================================================================================================================
// Types and helper functions
//======================================================================================================================

//! GEOS context for the spatial functions.
class GEOSLocalState : public FunctionLocalState {
public:
	GEOSContextHandle_t ctx;

	explicit GEOSLocalState() {
		ctx = GEOS_init_r();

		GEOSContext_setErrorMessageHandler_r(
		    ctx, [](const char *msg, void *) { throw InvalidInputException("GEOS error: %s", msg); }, nullptr);
	}
	~GEOSLocalState() override {
		GEOS_finish_r(ctx);
	}
};

//! Create a polygon geometry from the provided corner points.
static GEOSGeometry *CreatePolygon(GEOSContextHandle_t geos_ctx, const Point2D points[4]) {
	GEOSCoordSequence *coord_seq = GEOSCoordSeq_create_r(geos_ctx, 5, 2);

	if (!coord_seq) {
		throw std::runtime_error("Failed to create GEOS coordinate sequence");
	}

	GEOSCoordSeq_setX_r(geos_ctx, coord_seq, 0, points[0].x);
	GEOSCoordSeq_setY_r(geos_ctx, coord_seq, 0, points[0].y);
	GEOSCoordSeq_setX_r(geos_ctx, coord_seq, 1, points[1].x);
	GEOSCoordSeq_setY_r(geos_ctx, coord_seq, 1, points[1].y);
	GEOSCoordSeq_setX_r(geos_ctx, coord_seq, 2, points[2].x);
	GEOSCoordSeq_setY_r(geos_ctx, coord_seq, 2, points[2].y);
	GEOSCoordSeq_setX_r(geos_ctx, coord_seq, 3, points[3].x);
	GEOSCoordSeq_setY_r(geos_ctx, coord_seq, 3, points[3].y);
	// Close the polygon by repeating the first point
	GEOSCoordSeq_setX_r(geos_ctx, coord_seq, 4, points[0].x);
	GEOSCoordSeq_setY_r(geos_ctx, coord_seq, 4, points[0].y);

	GEOSGeometry *linear_ring = GEOSGeom_createLinearRing_r(geos_ctx, coord_seq);
	if (!linear_ring) {
		GEOSCoordSeq_destroy_r(geos_ctx, coord_seq);
		throw std::runtime_error("Failed to create GEOS linear ring");
	}

	GEOSGeometry *polygon = GEOSGeom_createPolygon_r(geos_ctx, linear_ring, nullptr, 0);
	if (!polygon) {
		GEOSGeom_destroy_r(geos_ctx, linear_ring);
		throw std::runtime_error("Failed to create GEOS polygon");
	}
	return polygon;
}

//======================================================================================================================
// RT_Envelope
//======================================================================================================================

struct RT_Envelope {
	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Compute the bounding box of the valid (non-no-data) cells in the input data cube and return it as a geometry.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 5);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		RasterTransformMatrix matrix;
		std::string matrix_str;

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			// Validate the input parameters.

			int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("band_index cannot be negative");
			}

			int32_t tile_x = args.data[2].GetValue(i).GetValue<int32_t>();
			if (tile_x < 0) {
				throw InvalidInputException("tile_x cannot be negative");
			}

			int32_t tile_y = args.data[3].GetValue(i).GetValue<int32_t>();
			if (tile_y < 0) {
				throw InvalidInputException("tile_y cannot be negative");
			}

			std::string metadata = args.data[4].GetValue(i).GetValue<string>();
			if (metadata != matrix_str) {
				matrix = RasterUtils::GetTransformMatrix(metadata);
				matrix_str = metadata;
			}

			// Calculate the envelope of the valid cells in the specified band.

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			const double(&gt)[6] = matrix.affine;
			const int32_t &blocksize_x = matrix.blocksize_x;
			const int32_t &blocksize_y = matrix.blocksize_y;

			RasterBounds bbox;

			if (!arg_cube.GetBounds(band_index, bbox)) {
				// No valid cells, return NULL geometry.
				Value geometry_null = Value();
				geometry_null.Reinterpret(LogicalType::GEOMETRY());
				result.SetValue(i, geometry_null);
			} else {
				int32_t tx = tile_x * blocksize_x;
				int32_t ty = tile_y * blocksize_y;
				Point2D pt0 = RasterUtils::RasterCoordToWorldCoord(gt, tx + bbox.min_col, ty + bbox.max_row + 1);
				Point2D pt1 = RasterUtils::RasterCoordToWorldCoord(gt, tx + bbox.max_col + 1, ty + bbox.max_row + 1);
				Point2D pt2 = RasterUtils::RasterCoordToWorldCoord(gt, tx + bbox.max_col + 1, ty + bbox.min_row);
				Point2D pt3 = RasterUtils::RasterCoordToWorldCoord(gt, tx + bbox.min_col, ty + bbox.min_row);

				const std::string geometry_wkt =
				    StringUtil::Format("POLYGON ((%f %f, %f %f, %f %f, %f %f, %f %f))", pt0.x, pt0.y, pt1.x, pt1.y,
				                       pt2.x, pt2.y, pt3.x, pt3.y, pt0.x, pt0.y);

				result.SetValue(i, geometry_wkt);
			}
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Computes the bounding box of the valid (non-no-data) cells in the input datacube for a specific band and returns it as a geometry.

		The function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The datacube column to polygonize. |
		| `band` | INTEGER | The 0-based index of the band to compute the envelope for. |
		| `tile_x` | INTEGER | The tile x coordinate of the tile. |
		| `tile_y` | INTEGER | The tile y coordinate of the tile. |
		| `metadata` | JSON | Raster metadata providing the affine geotransform matrix and tile block size. |

		The `metadata` argument is expected to contain the affine geotransform matrix and block size of the tile, which are used to convert between pixel coordinates and world coordinates.

		This argument can be populated with the `metadata` column returned by `RT_Read`, which contains the necessary information; specifically, a `transform` entry of array of 6 doubles representing the affine geotransform matrix, and a pair `blocksize_x`/`blocksize_y` of numeric values representing the
		block size of tile in `x` and `y` directions.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT
			RT_Envelope(databand_1, 0, tile_x, tile_y, metadata)
		FROM
			RT_Read('path/to/raster/file.tif')
		;
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "spatial");

		ScalarFunction function("RT_Envelope",
		                        {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
		                         LogicalType::INTEGER, LogicalType::JSON()},
		                        LogicalType::GEOMETRY(), Execute, nullptr, nullptr, nullptr);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//======================================================================================================================
// RT_Polygon
//======================================================================================================================

struct RT_Polygon {
	//------------------------------------------------------------------------------------------------------------------
	// Init Local
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<FunctionLocalState> InitLocal(ExpressionState &state, const BoundFunctionExpression &expr,
	                                                FunctionData *bind_data) {
		return make_uniq<GEOSLocalState>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Collects the coordinates of valid (non-no-data) cells of a specific band in a data cube.
	class CoordinatesCollector {
	public:
		CoordinatesCollector(GEOSContextHandle_t &geos_ctx) : geos_ctx(geos_ctx), has_no_data(false) {
		}

	public:
		//! Reset the collector to an empty state.
		void Reset() {
			bounds = RasterBounds();
			coords.clear();
			has_no_data = false;
		}

		//! Add the coordinates from the given data cube and band.
		void Add(DataCube &data_cube, const int32_t band, const int32_t tile_x, const int32_t tile_y,
		         const RasterTransformMatrix &matrix) {
			const DataHeader header = data_cube.GetHeader();
			const int32_t tx = tile_x * matrix.blocksize_x;
			const int32_t ty = tile_y * matrix.blocksize_y;

			// Collect coordinates.
			CubeCellFunc collect_coords = [this, &header, tx, ty](const CubeCellValue &v) {
				if (!v.IsNoDataValue()) {
					RasterCoord coord = v.GetCoord(header);
					coord.col += tx;
					coord.row += ty;
					coords.push_back(coord);
					bounds.Grow(coord);
				} else {
					has_no_data = true;
				}
			};
			DataCube::Apply(collect_coords, data_cube, band);
		}

		//! Convert the collected coordinates to a geometry Value.
		Value ToValue(const RasterTransformMatrix &matrix) const {
			const GEOSContextHandle_t geos_ctx = this->geos_ctx;
			const double(&gt)[6] = matrix.affine;
			Point2D points[4];

			auto geometry_free = [geos_ctx](GEOSGeometry *g) {
				if (g) {
					GEOSGeom_destroy_r(geos_ctx, g);
				}
			};
			auto wkb_writer_free = [geos_ctx](GEOSWKBWriter *w) {
				if (w) {
					GEOSWKBWriter_destroy_r(geos_ctx, w);
				}
			};
			auto wkb_free = [geos_ctx](unsigned char *p) {
				if (p) {
					GEOSFree_r(geos_ctx, p);
				}
			};

			std::shared_ptr<GEOSWKBWriter> wkb_writer(GEOSWKBWriter_create_r(geos_ctx), wkb_writer_free);
			GEOSWKBWriter_setOutputDimension_r(geos_ctx, wkb_writer.get(), 2);

			auto make_value = [geos_ctx, &wkb_writer, &wkb_free](GEOSGeometry *geom) {
				size_t wkb_size = 0;
				unsigned char *wkb_data = GEOSWKBWriter_write_r(geos_ctx, wkb_writer.get(), geom, &wkb_size);
				if (!wkb_data) {
					throw std::runtime_error("Failed to write geometry to WKB");
				}
				std::unique_ptr<unsigned char, decltype(wkb_free)> wkb_guard(wkb_data, wkb_free);
				Value geometry_val = Value::BLOB(wkb_data, wkb_size);
				geometry_val.Reinterpret(LogicalType::GEOMETRY());
				return geometry_val;
			};

			GEOSGeometry *polygon_ptr = nullptr;

			// No valid cells, return NULL geometry.
			if (coords.empty()) {
				Value geometry_null = Value();
				geometry_null.Reinterpret(LogicalType::GEOMETRY());
				return geometry_null;
			}
			// All cells are valid, return rectangle polygon for the whole tile.
			if (!has_no_data) {
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, bounds.min_col, bounds.max_row + 1);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, bounds.max_col + 1, bounds.max_row + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, bounds.max_col + 1, bounds.min_row);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, bounds.min_col, bounds.min_row);

				// Write to WKB and set the result value.
				polygon_ptr = CreatePolygon(geos_ctx, points);
				std::unique_ptr<GEOSGeometry, decltype(geometry_free)> polygon(polygon_ptr, geometry_free);
				Value geometry_val = make_value(polygon.get());
				return geometry_val;
			}

			// Mixed valid and no-data cells, create polygon for each valid cell and union them together.

			auto coord_to_polygon = [&](const RasterCoord &coord) {
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, coord.col, coord.row);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, coord.col, coord.row + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, coord.col + 1, coord.row + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, coord.col + 1, coord.row);
				return CreatePolygon(geos_ctx, points);
			};
			auto join_polygons = [&](GEOSGeometry *poly1, GEOSGeometry *poly2) {
				if (!poly1) {
					return poly2;
				}
				GEOSGeometry *new_polygon = GEOSUnion_r(geos_ctx, poly1, poly2);
				GEOSGeom_destroy_r(geos_ctx, poly1);
				GEOSGeom_destroy_r(geos_ctx, poly2);
				if (!new_polygon) {
					throw std::runtime_error("Failed to union geometries");
				}
				return new_polygon;
			};

			for (const RasterCoord &coord : coords) {
				GEOSGeometry *cell_polygon = coord_to_polygon(coord);
				polygon_ptr = join_polygons(polygon_ptr, cell_polygon);
			}

			// This should not happen, but just in case, return NULL geometry.
			if (!polygon_ptr) {
				Value geometry_null = Value();
				geometry_null.Reinterpret(LogicalType::GEOMETRY());
				return geometry_null;
			}

			// Return the result value.
			std::unique_ptr<GEOSGeometry, decltype(geometry_free)> polygon(polygon_ptr, geometry_free);
			Value geometry_val = make_value(polygon.get());
			return geometry_val;
		}

	private:
		GEOSContextHandle_t &geos_ctx;
		RasterBounds bounds;
		std::vector<RasterCoord> coords;
		bool has_no_data = false;
	};

	//! Create a polygon geometry for each contiguous region of non-no-data values in the data cube.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 5);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		GEOSLocalState &glocal_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSLocalState>();
		GEOSContextHandle_t &geos_ctx = glocal_state.ctx;
		CoordinatesCollector collector(geos_ctx);

		RasterTransformMatrix matrix;
		std::string matrix_str;

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			// Validate the input parameters.

			int32_t band_index = args.data[1].GetValue(i).GetValue<int32_t>();
			if (band_index < 0) {
				throw InvalidInputException("Band index cannot be negative");
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

			// Collect the coordinates of valid cells for the specified band.

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			collector.Reset();
			collector.Add(arg_cube, band_index, tile_x, tile_y, matrix);

			Value geometry_val = collector.ToValue(matrix);
			result.SetValue(i, geometry_val);
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Creates a polygon geometry for each contiguous region of non-no-data values in the datacube.

		This function takes a datacube column as input and returns polygon geometry representing the contiguous regions of non-no-data values in the datacube. The function needs the tile coordinates, Geo Transform matrix, and blocksize of the datacube to calculate the geometry of the output polygons.

		The function accepts the following parameters:

		| Parameter | Type | Description |
		| --------- | -----| ----------- |
		| `databand` | DATACUBE | The datacube column to polygonize. |
		| `band` | INTEGER | The band index to polygonize. |
		| `tile_x` | INTEGER | The tile x coordinate of the tile. |
		| `tile_y` | INTEGER | The tile y coordinate of the tile. |
		| `metadata` | JSON | Raster metadata providing the affine geotransform matrix and tile block size. |

		The `metadata` argument is expected to contain the affine geotransform matrix and block size of the tile, which are used to convert between pixel coordinates and world coordinates.

		This argument can be populated with the `metadata` column returned by `RT_Read`, which contains the necessary information; specifically, a `transform` entry of array of 6 doubles representing the affine geotransform matrix, and a pair `blocksize_x`/`blocksize_y` of numeric values representing the
		block size of tile in `x` and `y` directions.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT
            RT_Polygon(databand_1, 0, tile_x, tile_y, metadata)
		FROM
			RT_Read('path/to/raster/file.tif')
		;
	)";

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "spatial");

		ScalarFunction function("RT_Polygon",
		                        {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
		                         LogicalType::INTEGER, LogicalType::JSON()},
		                        LogicalType::GEOMETRY(), Execute, nullptr, nullptr, nullptr, InitLocal);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

//======================================================================================================================
// RT_SpatialOp
//======================================================================================================================

struct RT_SpatialOp {
	//! Supported spatial operations that can be applied to data cubes.
	enum SpatialOp : uint8_t {
		CLIP = 0,
		BURN = 1,
	};

	//------------------------------------------------------------------------------------------------------------------
	// Init Local
	//------------------------------------------------------------------------------------------------------------------

	static unique_ptr<FunctionLocalState> InitLocal(ExpressionState &state, const BoundFunctionExpression &expr,
	                                                FunctionData *bind_data) {
		return make_uniq<GEOSLocalState>();
	}

	//------------------------------------------------------------------------------------------------------------------
	// Execute
	//------------------------------------------------------------------------------------------------------------------

	//! Execute a spatial operation on the input data cube.
	static void Execute(const SpatialOp &op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

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
		auto wkb_reader_free = [geos_ctx](GEOSWKBReader *r) {
			if (r) {
				GEOSWKBReader_destroy_r(geos_ctx, r);
			}
		};

		std::shared_ptr<GEOSWKBReader> wkb_reader(GEOSWKBReader_create_r(geos_ctx), wkb_reader_free);
		std::unique_ptr<GEOSGeometry, decltype(geometry_free)> raw_geom(nullptr, geometry_free);
		std::unique_ptr<const GEOSPreparedGeometry, decltype(prep_geom_free)> prep_geom(nullptr, prep_geom_free);
		bool geometry_is_constant = false;
		Point2D points[4];

		auto extract_geometry = [&](Value arg_value, std::unique_ptr<GEOSGeometry, decltype(geometry_free)> &raw_geom,
		                            std::unique_ptr<const GEOSPreparedGeometry, decltype(prep_geom_free)> &prep_geom) {
			const string &wkb_str = StringValue::Get(arg_value);
			const_data_ptr_t data_ptr = const_data_ptr_t(wkb_str.data());
			raw_geom.reset(GEOSWKBReader_read_r(geos_ctx, wkb_reader.get(), data_ptr, wkb_str.size()));
			if (!raw_geom) {
				throw InvalidInputException("Failed to parse input geometry WKB");
			}
			prep_geom.reset(GEOSPrepare_r(geos_ctx, raw_geom.get()));
		};

		if (count > 0 && args.data[4].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			extract_geometry(args.data[4].GetValue(0), raw_geom, prep_geom);
			geometry_is_constant = true;
		}

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.

		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			// Validate the input parameters.

			int32_t tile_x = args.data[1].GetValue(i).GetValue<int32_t>();
			if (tile_x < 0) {
				throw InvalidInputException("Tile X coordinate cannot be negative");
			}

			int32_t tile_y = args.data[2].GetValue(i).GetValue<int32_t>();
			if (tile_y < 0) {
				throw InvalidInputException("Tile Y coordinate cannot be negative");
			}

			std::string metadata = args.data[3].GetValue(i).GetValue<string>();
			if (metadata != matrix_str) {
				matrix = RasterUtils::GetTransformMatrix(metadata);
				matrix_str = metadata;
			}

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			const double(&gt)[6] = matrix.affine;
			const int32_t &blocksize_x = matrix.blocksize_x;
			const int32_t &blocksize_y = matrix.blocksize_y;

			// Parse input geometry?
			if (!geometry_is_constant) {
				extract_geometry(args.data[4].GetValue(i), raw_geom, prep_geom);
			}
			if (!prep_geom) {
				throw InvalidInputException("Failed to prepare geometry for row %lu", i);
			}

			// Evaluate the spatial operation on the data cube.

			const DataHeader header = arg_cube.GetHeader();
			double burn_value = args.data[5].GetValue(i).GetValue<double>();

			auto coord_to_polygon = [&](const RasterCoord &coord) {
				int32_t tx = tile_x * blocksize_x + coord.col;
				int32_t ty = tile_y * blocksize_y + coord.row;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);
				return CreatePolygon(geos_ctx, points);
			};

			auto evaluate_geometry = [&](const CubeCellValue &v, double &result) {
				RasterCoord coord = v.GetCoord(header);

				GEOSGeometry *polygon_ptr = coord_to_polygon(coord);
				std::unique_ptr<GEOSGeometry, decltype(geometry_free)> polygon(polygon_ptr, geometry_free);

				int inside = GEOSPreparedIntersects_r(geos_ctx, prep_geom.get(), polygon_ptr);
				if (op == SpatialOp::CLIP) {
					result = (inside == 1) ? v.value : burn_value;
				} else {
					result = (inside == 1) ? burn_value : v.value;
				}
				return true;
			};
			arg_cube.Apply(evaluate_geometry, arg_cube, res_cube);

			// Free per-row geometry (not the shared constant one).

			if (!geometry_is_constant) {
				prep_geom.reset();
				raw_geom.reset();
			}

			// Set the result.

			result.SetValue(i, res_cube.ToBlob());
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Register
	//------------------------------------------------------------------------------------------------------------------

	static void Register(ExtensionLoader &loader) {
		InsertionOrderPreservingMap<string> tags;
		tags.insert("ext", "raster");
		tags.insert("category", "spatial");

		// Register Spatial functions

		static constexpr std::array<std::tuple<const char *, SpatialOp, const char *>, 2> spatial_ops = {{
		    {"RT_CubeClip", SpatialOp::CLIP,
		     "Returns a datacube where cells outside the given geometry are replaced by the specified value. "
		     "Cells inside the geometry are preserved. Nodata cells are preserved."},
		    {"RT_CubeBurn", SpatialOp::BURN,
		     "Returns a datacube where cells inside the given geometry are replaced by the specified value. "
		     "Cells outside the geometry are preserved. Nodata cells are preserved."},
		}};
		for (const auto &entry : spatial_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);

			const auto executor = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_SpatialOp::Execute(op, args, state, result);
			};
			ScalarFunction function =
			    ScalarFunction(function_name,
			                   {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
			                    LogicalType::JSON(), LogicalType::GEOMETRY(), LogicalType::DOUBLE},
			                   RasterTypes::DATACUBE(), executor, nullptr, nullptr, nullptr, InitLocal);

			RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, description, "",
			                                 tags);
		}
	}
};

} // namespace

// #####################################################################################################################
// Register Spatial Functions
// #####################################################################################################################

void RasterSpatialFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_Envelope::Register(loader);
	RT_Polygon::Register(loader);
	RT_SpatialOp::Register(loader);
}

} // namespace duckdb
