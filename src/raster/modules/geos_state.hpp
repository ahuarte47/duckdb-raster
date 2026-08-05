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

	//! Convert a GEOS geometry to a DuckDB Value of type GEOMETRY.
	static Value GeometryToValue(const GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry);
	//! Convert a GEOS geometry to a DuckDB Value of type GEOMETRY.
	Value GeometryToValue(GEOSGeometry *geometry);

	//! Get the bounding box of a GEOS geometry as a GeometryExtent.
	static GeometryExtent GetGeometryExtent(GEOSContextHandle_t geos_ctx, GEOSGeometry *geometry);
	//! Get the bounding box of a GEOS geometry as a GeometryExtent.
	GeometryExtent GetGeometryExtent(GEOSGeometry *geometry);

	//! Check if a GeometryExtent intersects with a polygon defined by the provided corner points.
	static bool Intersects(const GeometryExtent &geom_extent, const Point2D points[4]);

	//! Check if a GEOS geometry intersects with a polygon defined by the provided corner points.
	static bool Intersects(GEOSContextHandle_t geos_ctx, const GEOSPreparedGeometry *geometry, const Point2D points[4]);
	//! Check if a GEOS geometry intersects with a polygon defined by the provided corner points.
	bool Intersects(const GEOSPreparedGeometry *geometry, const Point2D points[4]);
};

} // namespace duckdb
