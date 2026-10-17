#pragma once

#include <cstdint>
#include "raster_types.hpp"

// DuckDB
#include "duckdb.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

//! An interface representing a row of attributes for filter evaluation.
class IFilterRow {
public:
	virtual ~IFilterRow() = default;

	//! Get the value types of the columns in the rows.
	virtual vector<LogicalType> ValueTypesOf(const vector<column_t> &column_ids,
	                                         const vector<LogicalType> &column_types) const = 0;

	//! Get the value of a column in the row by index.
	virtual const Value &ValueOf(int column_index) const = 0;
};

//! Class representing a row of attributes of a raster tile.
class RasterRow : public IFilterRow {
private:
	//! A constant empty value to return for invalid column indices.
	static const Value EMPTY_VALUE;

public:
	//! Constructor for RasterRow.
	RasterRow(const Value &row_id, const Value &x, const Value &y, const Value &bbox, const Value &geometry,
	          const Value &level, const Value &tile_x, const Value &tile_y, const Value &size_x, const Value &size_y,
	          const Value &metadata_ds)
	    : row_id(row_id), x(x), y(y), bbox(bbox), geometry(geometry), level(level), tile_x(tile_x), tile_y(tile_y),
	      size_x(size_x), size_y(size_y), metadata_ds(metadata_ds) {
	}

	const Value row_id;
	const Value x;
	const Value y;
	const Value bbox;
	const Value geometry;
	const Value level;
	const Value tile_x;
	const Value tile_y;
	const Value size_x;
	const Value size_y;
	const Value metadata_ds;

	//! Get the value types of the columns in the rows.
	virtual vector<LogicalType> ValueTypesOf(const vector<column_t> &column_ids,
	                                         const vector<LogicalType> &column_types) const override;

	//! Get the value of a column in the row by index.
	const Value &ValueOf(int column_index) const override;
};

//! Class representing a row of attributes of a raster cell.
class CellRow : public IFilterRow {
private:
	//! A constant empty value to return for invalid column indices.
	static const Value EMPTY_VALUE;

public:
	//! Constructor for CellRow.
	CellRow(const Value &row_id, const Value &x, const Value &y, const Value &geometry, const Value &col,
	        const Value &row)
	    : row_id(row_id), x(x), y(y), geometry(geometry), col(col), row(row) {
	}

	const Value row_id;
	const Value x;
	const Value y;
	const Value geometry;
	const Value col;
	const Value row;

	//! Get the value types of the columns in the rows.
	virtual vector<LogicalType> ValueTypesOf(const vector<column_t> &column_ids,
	                                         const vector<LogicalType> &column_types) const override;

	//! Get the value of a column in the row by index.
	const Value &ValueOf(int column_index) const override;
};

/**
 * Evaluates filter expressions on raster items.
 */
class FilterEval {
public:
	FilterEval(ClientContext &context, const vector<unique_ptr<Expression>> &expressions,
	           const vector<column_t> &column_ids, const vector<LogicalType> &column_types)
	    : context(context), expressions(expressions), column_ids(column_ids), column_types(column_types) {
	}

private:
	//! The client context for expression execution.
	ClientContext &context;
	//! The set of filter expressions to evaluate.
	const vector<unique_ptr<Expression>> &expressions;
	//! The map column indices to table columns.
	const vector<column_t> &column_ids;
	//! The types of the columns.
	const vector<LogicalType> &column_types;

	//! The expression executor used for evaluating the filter expressions.
	std::unique_ptr<ExpressionExecutor> executor;
	//! The input DataChunk.
	DataChunk input;
	//! The result DataChunk.
	DataChunk result;

	//! Get the expression executor, initializing it if necessary.
	ExpressionExecutor *GetExecutor(const IFilterRow &row);

public:
	/**
	 * Evaluate a set of DuckDB expressions on a raster item.
	 *
	 * Returns True if the filter was successfully evaluated or not totally supported by the evaluator.
	 * False means we must skip the current tile in the table scan.
	 *
	 * In case of unsupported expressions, the filter will be evaluated at a later stage by
	 * the DuckDB runtime engine.
	 */
	bool Eval(const IFilterRow &row);
};

} // namespace duckdb
