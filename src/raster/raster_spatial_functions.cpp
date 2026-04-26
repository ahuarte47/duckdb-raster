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
// RT_CubePolygonize
//======================================================================================================================

struct RT_CubePolygonize {
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
	static void Polygonize(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.data.size() == 6);
		const idx_t count = args.size();

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
		RestoreConstantVectorIfNeeded(args, result);
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
			RT_CubePolygonize(databand_1,
							  tile_x,
							  tile_y,
						     (metadata->>'transform')::DOUBLE[],
							 (metadata->>'blocksize_x')::INTEGER,
							 (metadata->>'blocksize_y')::INTEGER)
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

		ScalarFunction function("RT_CubePolygonize",
		                        {RasterTypes::DATACUBE(), LogicalType::INTEGER, LogicalType::INTEGER,
		                         LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::DOUBLE)},
		                        LogicalType::GEOMETRY(), Polygonize, nullptr, nullptr, nullptr, InitLocal);

		RegisterFunction<ScalarFunction>(loader, function, CatalogType::SCALAR_FUNCTION_ENTRY, DESCRIPTION, EXAMPLE,
		                                 tags);
	}
};

} // namespace

// #####################################################################################################################
// Register Spatial Functions
// #####################################################################################################################

void RasterSpatialFunctions::Register(ExtensionLoader &loader) {
	// Register functions
	RT_CubePolygonize::Register(loader);
}

} // namespace duckdb
