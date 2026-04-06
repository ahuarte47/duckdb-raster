#pragma once

#include <cstdint>
#include "raster_types.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

/**
 * Context for expression evaluation on a raster tile set of attributes.
 */
class FilterContext {
public:
	FilterContext(ClientContext &context, const vector<std::unique_ptr<Expression>> &expressions,
	              const vector<column_t> &column_ids, const vector<LogicalType> &column_types)
	    : context(context), expressions(expressions), column_ids(column_ids), column_types(column_types) {
	}

public:
	//! The client context for expression execution.
	ClientContext &context;
	//! The set of filter expressions to evaluate.
	const vector<std::unique_ptr<Expression>> &expressions;
	//! The map column indices to table columns.
	const vector<column_t> &column_ids;
	//! The types of the columns.
	const vector<LogicalType> &column_types;
};

//! Struct representing a raster row's attributes for filter evaluation.
class RasterRow {
private:
	//! A constant empty value to return for invalid column indices.
	static const Value EMPTY_VALUE;

public:
	const Value &row_id;
	const Value &x;
	const Value &y;
	const Value &bbox;
	const Value &geometry;
	const Value &level;
	const Value &tile_x;
	const Value &tile_y;
	const Value &size_x;
	const Value &size_y;
	const Value &metadata_ds;

	//! Get the value of a column by index for filter evaluation.
	const Value &ValueOf(int column_index) const {
		switch (column_index) {
		case RASTER_ROWID_COLUMN_INDEX:
			return row_id;
		case RASTER_X_COLUMN_INDEX:
			return x;
		case RASTER_Y_COLUMN_INDEX:
			return y;
		case RASTER_BBOX_COLUMN_INDEX:
			return bbox;
		case RASTER_GEOMETRY_COLUMN_INDEX:
			return geometry;
		case RASTER_LEVEL_COLUMN_INDEX:
			return level;
		case RASTER_COL_COLUMN_INDEX:
			return tile_x;
		case RASTER_ROW_COLUMN_INDEX:
			return tile_y;
		case RASTER_WIDTH_COLUMN_INDEX:
			return size_x;
		case RASTER_HEIGHT_COLUMN_INDEX:
			return size_y;
		case RASTER_METADATA_COLUMN_INDEX:
			return metadata_ds;
		default:
			return EMPTY_VALUE;
		}
	}
};

/**
 * Evaluation class for FilterPushDown.
 * Evaluates filter expressions on a raster tile set of attributes.
 */
class FilterEval {
public:
	/**
	 * Evaluate a set of DuckDB expressions on a raster tile set of attributes.
	 *
	 * Returns True if the filter was successfully evaluated or not totally supported by the evaluator.
	 * False means we must skip the current tile in the table scan.
	 *
	 * In case of unsupported expressions, the filter will be evaluated at a later stage by
	 * the DuckDB runtime engine.
	 */
	static bool Eval(const RasterRow &row, const FilterContext &ctx);
};

} // namespace duckdb
