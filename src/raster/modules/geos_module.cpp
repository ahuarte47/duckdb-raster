#include "geos_module.hpp"

namespace duckdb {

GEOSLocalState::GEOSLocalState() {
	ctx = GEOS_init_r();

	GEOSContext_setErrorMessageHandler_r(
	    ctx, [](const char *msg, void *) { throw InvalidInputException("GEOS error: %s", msg); }, nullptr);
}

GEOSLocalState::~GEOSLocalState() {
	GEOS_finish_r(ctx);
}

GEOSGeometry *GEOSLocalState::CreatePolygon(GEOSContextHandle_t geos_ctx, const Point2D points[4]) {
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

GEOSGeometry *GEOSLocalState::CreatePolygon(const Point2D points[4]) {
	return CreatePolygon(ctx, points);
}

GEOSGeometry *GEOSLocalState::CreateGeometry(GEOSContextHandle_t geos_ctx, const Value &value) {
	auto wkb_reader_free = [geos_ctx](GEOSWKBReader *r) {
		if (r) {
			GEOSWKBReader_destroy_r(geos_ctx, r);
		}
	};

	std::shared_ptr<GEOSWKBReader> wkb_reader(GEOSWKBReader_create_r(geos_ctx), wkb_reader_free);
	const string &wkb_str = StringValue::Get(value);
	const_data_ptr_t data_ptr = const_data_ptr_t(wkb_str.data());

	GEOSGeometry *geometry = GEOSWKBReader_read_r(geos_ctx, wkb_reader.get(), data_ptr, wkb_str.size());
	if (!geometry) {
		throw std::runtime_error("Failed to create GEOS geometry from WKB");
	}

	return geometry;
}

GEOSGeometry *GEOSLocalState::CreateGeometry(const Value &value) {
	return CreateGeometry(ctx, value);
}

GEOSGeometry *GEOSLocalState::CreateGeometry(GEOSContextHandle_t geos_ctx, const string_t &wkb) {
	auto wkb_reader_free = [geos_ctx](GEOSWKBReader *r) {
		if (r) {
			GEOSWKBReader_destroy_r(geos_ctx, r);
		}
	};

	std::shared_ptr<GEOSWKBReader> wkb_reader(GEOSWKBReader_create_r(geos_ctx), wkb_reader_free);
	const_data_ptr_t data_ptr = const_data_ptr_cast(wkb.GetData());

	GEOSGeometry *geometry = GEOSWKBReader_read_r(geos_ctx, wkb_reader.get(), data_ptr, wkb.GetSize());
	if (!geometry) {
		throw std::runtime_error("Failed to create GEOS geometry from WKB");
	}

	return geometry;
}

GEOSGeometry *GEOSLocalState::CreateGeometry(const string_t &wkb) {
	return CreateGeometry(ctx, wkb);
}

Value GEOSLocalState::GeometryToValue(const GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry) {
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

	size_t wkb_size = 0;
	unsigned char *wkb_data = GEOSWKBWriter_write_r(geos_ctx, wkb_writer.get(), geometry, &wkb_size);
	if (!wkb_data) {
		throw std::runtime_error("Failed to write geometry to WKB");
	}

	std::unique_ptr<unsigned char, decltype(wkb_free)> wkb_guard(wkb_data, wkb_free);
	Value geometry_val = Value::BLOB(wkb_data, wkb_size);
	geometry_val.Reinterpret(LogicalType::GEOMETRY());

	return geometry_val;
}

Value GEOSLocalState::GeometryToValue(GEOSGeometry *geometry) {
	return GeometryToValue(ctx, geometry);
}

GeometryExtent GEOSLocalState::GetGeometryExtent(GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry) {
	double x_min = 0.0;
	double y_min = 0.0;
	double x_max = 0.0;
	double y_max = 0.0;

	if (!GEOSGeom_getExtent_r(geos_ctx, geometry, &x_min, &y_min, &x_max, &y_max)) {
		throw std::runtime_error("Failed to get geometry extent");
	}

	GeometryExtent extent = {x_min, y_min, 0.0, 0.0, x_max, y_max, 0.0, 0.0};
	return extent;
}

GeometryExtent GEOSLocalState::GetGeometryExtent(GEOSGeometry *geometry) {
	return GetGeometryExtent(ctx, geometry);
}

GEOSIntersectsGeometry::GEOSIntersectsGeometry(GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry)
    : geos_ctx(geos_ctx), geometry(geometry), prep_geometry(GEOSPrepare_r(geos_ctx, geometry)),
      geom_extent(GEOSLocalState::GetGeometryExtent(geos_ctx, geometry)) {
}

GEOSIntersectsGeometry::~GEOSIntersectsGeometry() {
	if (prep_geometry) {
		GEOSPreparedGeom_destroy_r(geos_ctx, prep_geometry);
	}
	if (geometry) {
		GEOSGeom_destroy_r(geos_ctx, geometry);
	}
}

bool GEOSIntersectsGeometry::Intersects(const Point2D points[4]) {
	double x_min = std::min({points[0].x, points[1].x, points[2].x, points[3].x});
	double y_min = std::min({points[0].y, points[1].y, points[2].y, points[3].y});
	double x_max = std::max({points[0].x, points[1].x, points[2].x, points[3].x});
	double y_max = std::max({points[0].y, points[1].y, points[2].y, points[3].y});

	GeometryExtent bbox = {x_min, y_min, 0, 0, x_max, y_max, 0, 0};
	if (!geom_extent.IntersectsXY(bbox)) {
		return false;
	}

	GEOSGeometry *polygon_ptr = GEOSLocalState::CreatePolygon(geos_ctx, points);
	if (!polygon_ptr) {
		throw std::runtime_error("Failed to create GEOS polygon from corner points");
	}

	if (prep_geometry) {
		int inside = GEOSPreparedIntersects_r(geos_ctx, prep_geometry, polygon_ptr);
		GEOSGeom_destroy_r(geos_ctx, polygon_ptr);
		return inside == 1;
	} else {
		int inside = GEOSIntersects_r(geos_ctx, geometry, polygon_ptr);
		GEOSGeom_destroy_r(geos_ctx, polygon_ptr);
		return inside == 1;
	}
}

} // namespace duckdb
