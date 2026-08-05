#pragma once

#include "raster_types.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/scalar_function.hpp"

// GEOS
#include "geos_c.h"

namespace duckdb {

//! GEOS context for the spatial functions.
class GEOSLocalState : public FunctionLocalState {
public:
	GEOSContextHandle_t ctx;

	explicit GEOSLocalState();
	~GEOSLocalState() override;

public:
	//! Create a polygon geometry from the provided corner points.
	static GEOSGeometry *CreatePolygon(GEOSContextHandle_t geos_ctx, const Point2D points[4]);
	//! Create a polygon geometry from the provided corner points.
	GEOSGeometry *CreatePolygon(const Point2D points[4]);

	//! Create a GEOS geometry from a DuckDB Value of type GEOMETRY.
	static GEOSGeometry *CreateGeometry(GEOSContextHandle_t geos_ctx, const Value &value);
	//! Create a GEOS geometry from a DuckDB Value of type GEOMETRY.
	GEOSGeometry *CreateGeometry(const Value &value);

	//! Create a GEOS geometry from a raw WKB string_t (avoids implicit Value conversion that validates UTF-8).
	static GEOSGeometry *CreateGeometry(GEOSContextHandle_t geos_ctx, const string_t &wkb);
	//! Create a GEOS geometry from a raw WKB string_t.
	GEOSGeometry *CreateGeometry(const string_t &wkb);

	//! Convert a GEOS geometry to a DuckDB Value of type GEOMETRY.
	static Value GeometryToValue(const GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry);
	//! Convert a GEOS geometry to a DuckDB Value of type GEOMETRY.
	Value GeometryToValue(GEOSGeometry *geometry);

	//! Get the bounding box of a GEOS geometry as a GeometryExtent.
	static GeometryExtent GetGeometryExtent(GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry);
	//! Get the bounding box of a GEOS geometry as a GeometryExtent.
	GeometryExtent GetGeometryExtent(GEOSGeometry *geometry);
};

//! A wrapper around a GEOS geometry with custom methods to check for intersections with other geometries.
class GEOSIntersectsGeometry {
public:
	GEOSIntersectsGeometry(GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry);
	~GEOSIntersectsGeometry();

private:
	GEOSContextHandle_t geos_ctx;
	GEOSGeometry *geometry;
	const GEOSPreparedGeometry *prep_geometry;
	GeometryExtent geom_extent;

public:
	//! Check if this geometry intersects with a polygon defined by the provided corner points.
	bool Intersects(const Point2D points[4]);
};

} // namespace duckdb
