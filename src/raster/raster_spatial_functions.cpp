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

//! Extract the geo transform parameters from the input argument and store them in the provided array.
static void ExtractGeoTransform(const Value &geo_transform, double gt[6]) {
	const auto &children = ListValue::GetChildren(geo_transform);

	if (children.size() != 6) {
		throw InvalidInputException(
		    "geo_transform must have exactly 6 values: originX, pixelWidth, rotX, originY, rotY, pixelHeight.");
	}
	for (idx_t k = 0; k < 6; k++) {
		gt[k] = children[k].GetValue<double>();
	}
}

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
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			int32_t tile_x = args.data[1].GetValue(i).GetValue<int32_t>();
			int32_t tile_y = args.data[2].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_x = args.data[3].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_y = args.data[4].GetValue(i).GetValue<int32_t>();

			double gt[6] = {0};
			ExtractGeoTransform(args.data[5].GetValue(i), gt);

			RasterBounds bbox;

			if (!arg_cube.GetBounds(bbox)) {
				// No valid cells, return NULL geometry.
				result.SetValue(i, Value());
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
		Computes the bounding box of the valid (non-no-data) cells in the input data cube and
		returns it as a geometry.

		The fourth argument is a list of 6 doubles: [originX, pixelWidth, rotX, originY, rotY, pixelHeight],
		it represents the geo_transform of the input data cube, which is used to convert pixel coordinates to
		spatial coordinates.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT
			RT_Envelope(databand_1,
                        tile_x,
                        tile_y,
                       (metadata->>'blocksize_x')::INTEGER,
                       (metadata->>'blocksize_y')::INTEGER,
                       (metadata->>'transform')::DOUBLE[])
		FROM
			my_raster_table
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
		                         LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::DOUBLE)},
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

	struct RasterCoordHash {
		size_t operator()(const RasterCoord &c) const {
			return std::hash<int64_t>()(static_cast<int64_t>(c.row) << 32 | static_cast<uint32_t>(c.col));
		}
	};
	struct RasterCoordEqual {
		bool operator()(const RasterCoord &a, const RasterCoord &b) const {
			return a == b;
		}
	};

	//! Create a polygon geometry for each contiguous region of non-no-data values in the data cube.
	static void Execute(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));

		// Functions to hold the spatial coordinates of cells for the current row.

		GEOSLocalState &glocal_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSLocalState>();
		GEOSContextHandle_t geos_ctx = glocal_state.ctx;

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

		std::vector<RasterCoord> coords_vec;
		std::unordered_set<RasterCoord, RasterCoordHash, RasterCoordEqual> coords_set;
		Point2D points[4];

		auto make_value = [geos_ctx, &wkb_writer, &wkb_free](GEOSGeometry *geom) {
			size_t wkb_size = 0;
			unsigned char *wkb_data = GEOSWKBWriter_write_r(geos_ctx, wkb_writer.get(), geom, &wkb_size);
			if (!wkb_data) {
				throw std::runtime_error("Failed to write geometry to WKB");
			}
			std::unique_ptr<unsigned char, decltype(wkb_free)> wkb_guard(wkb_data, wkb_free);
			Value geom_val = Value::BLOB(wkb_data, wkb_size);
			geom_val.Reinterpret(LogicalType::GEOMETRY());
			return geom_val;
		};

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.
		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			int32_t tile_x = args.data[1].GetValue(i).GetValue<int32_t>();
			int32_t tile_y = args.data[2].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_x = args.data[3].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_y = args.data[4].GetValue(i).GetValue<int32_t>();

			double gt[6] = {0};
			ExtractGeoTransform(args.data[5].GetValue(i), gt);

			const DataHeader header = arg_cube.GetHeader();
			coords_vec.clear();
			coords_set.clear();

			// Collect coordinates for all non-no-data cells of the specified band.
			CubeCellFunc collect_coords = [&](const CubeCellValue &v) {
				if (!v.IsNoDataValue()) {
					RasterCoord coord = v.GetCoord(header);

					if (header.bands == 1) {
						coords_vec.push_back(coord);
					} else {
						coords_set.insert(coord);
					}
				}
			};
			DataCube::Apply(collect_coords, arg_cube);

			// Join the cell coordinates into contiguous regions and create polygons for each region.

			GEOSGeometry *polygon_ptr = nullptr;

			if (coords_vec.empty() && coords_set.empty()) {
				// No valid cells, return NULL geometry.
				Value null_geom = Value();
				null_geom.Reinterpret(LogicalType::GEOMETRY());
				result.SetValue(i, null_geom);
				continue;
			}
			if (coords_vec.size() == (static_cast<idx_t>(header.cols) * header.rows) ||
			    coords_set.size() == (static_cast<idx_t>(header.cols) * header.rows)) {
				// All cells are valid, return rectangle polygon for the whole tile.
				int32_t tx = tile_x * blocksize_x;
				int32_t ty = tile_y * blocksize_y;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + header.rows);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + header.cols, ty + header.rows);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + header.cols, ty);

				// Write to WKB and set the result value.
				polygon_ptr = CreatePolygon(geos_ctx, points);
				std::unique_ptr<GEOSGeometry, decltype(geometry_free)> polygon(polygon_ptr, geometry_free);
				Value geom_val = make_value(polygon.get());
				result.SetValue(i, geom_val);
				continue;
			}

			auto coord_to_polygon = [&](const RasterCoord &coord) {
				int32_t tx = tile_x * blocksize_x + coord.col;
				int32_t ty = tile_y * blocksize_y + coord.row;
				points[0] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty);
				points[1] = RasterUtils::RasterCoordToWorldCoord(gt, tx, ty + 1);
				points[2] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty + 1);
				points[3] = RasterUtils::RasterCoordToWorldCoord(gt, tx + 1, ty);
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

			if (header.bands == 1) {
				for (const RasterCoord &coord : coords_vec) {
					GEOSGeometry *cell_polygon = coord_to_polygon(coord);
					polygon_ptr = join_polygons(polygon_ptr, cell_polygon);
				}
			} else {
				for (const RasterCoord &coord : coords_set) {
					GEOSGeometry *cell_polygon = coord_to_polygon(coord);
					polygon_ptr = join_polygons(polygon_ptr, cell_polygon);
				}
			}

			if (!polygon_ptr) {
				RASTER_SCAN_DEBUG_LOG(1, "Failed to create polygon for row %lu", i);

				// This should not happen, but just in case, return NULL geometry.
				Value null_geom = Value();
				null_geom.Reinterpret(LogicalType::GEOMETRY());
				result.SetValue(i, null_geom);
				continue;
			}

			std::unique_ptr<GEOSGeometry, decltype(geometry_free)> polygon(polygon_ptr, geometry_free);
			Value geom_val = make_value(polygon.get());
			result.SetValue(i, geom_val);
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// Documentation
	//------------------------------------------------------------------------------------------------------------------

	static constexpr auto DESCRIPTION = R"(
		Creates a polygon geometry for each contiguous region of non-no-data values in the data cube.

		The fourth argument is a list of 6 doubles: [originX, pixelWidth, rotX, originY, rotY, pixelHeight],
		it represents the geo_transform of the input data cube, which is used to convert pixel coordinates to
		spatial coordinates.
	)";

	static constexpr auto EXAMPLE = R"(
		SELECT
            RT_Polygon(databand_1,
                       tile_x,
                       tile_y,
                      (metadata->>'blocksize_x')::INTEGER,
                      (metadata->>'blocksize_y')::INTEGER,
                      (metadata->>'transform')::DOUBLE[])
		FROM
			my_raster_table
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
		                         LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::DOUBLE)},
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
	// Function
	//------------------------------------------------------------------------------------------------------------------

	//! Evaluate a spatial operation on the input data cube.
	static void EvalSpatialOp(const SpatialOp &op, DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 8);
		const idx_t count = args.size();
		args.Flatten();

		DataCube arg_cube(Allocator::Get(state.GetContext()));
		DataCube res_cube(Allocator::Get(state.GetContext()));

		// If the transform matrix argument is constant across the whole chunk, parse it once.

		double gt[6] = {0};
		bool gt_is_constant = false;

		if (count > 0 && args.data[5].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			ExtractGeoTransform(args.data[5].GetValue(0), gt);
			gt_is_constant = true;
		}

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

		if (count > 0 && args.data[6].GetVectorType() == VectorType::CONSTANT_VECTOR) {
			extract_geometry(args.data[6].GetValue(0), raw_geom, prep_geom);
			geometry_is_constant = true;
		}

		// We loop over rows manually because DuckDB Executors only support C++ primitive types.

		for (idx_t i = 0; i < count; i++) {
			Value blob = args.data[0].GetValue(i);

			arg_cube.LoadBlob(blob);
			arg_cube.EnsureRaw();

			int32_t tile_x = args.data[1].GetValue(i).GetValue<int32_t>();
			int32_t tile_y = args.data[2].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_x = args.data[3].GetValue(i).GetValue<int32_t>();
			int32_t blocksize_y = args.data[4].GetValue(i).GetValue<int32_t>();

			// Parse input geo transform matrix?
			if (!gt_is_constant) {
				ExtractGeoTransform(args.data[5].GetValue(i), gt);
			}

			// Parse input geometry?
			if (!geometry_is_constant) {
				extract_geometry(args.data[6].GetValue(i), raw_geom, prep_geom);
			}
			if (!prep_geom) {
				throw InvalidInputException("Failed to prepare geometry for row %lu", i);
			}

			// Evaluate the spatial operation on the data cube.

			const DataHeader header = arg_cube.GetHeader();
			double burn_value = args.data[7].GetValue(i).GetValue<double>();

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
		     "Returns a data cube where cells outside the given geometry are replaced by the specified value. "
		     "Cells inside the geometry are preserved. No-data cells are preserved."},
		    {"RT_CubeBurn", SpatialOp::BURN,
		     "Returns a data cube where cells inside the given geometry are replaced by the specified value. "
		     "Cells outside the geometry are preserved. No-data cells are preserved."},
		}};
		for (const auto &entry : spatial_ops) {
			const auto &function_name = std::get<0>(entry);
			const auto &op = std::get<1>(entry);
			const auto &description = std::get<2>(entry);

			const auto executor = [op](DataChunk &args, ExpressionState &state, Vector &result) {
				RT_SpatialOp::EvalSpatialOp(op, args, state, result);
			};
			ScalarFunction function =
			    ScalarFunction(function_name,
			                   {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
			                    LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::DOUBLE),
			                    LogicalType::GEOMETRY(), LogicalType::DOUBLE},
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
