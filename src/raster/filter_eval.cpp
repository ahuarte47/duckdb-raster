#include "raster_types.hpp"
#include "raster_utils.hpp"
#include "filter_eval.hpp"

// DuckDB
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

namespace duckdb {

//======================================================================================================================
// FilterEval
//======================================================================================================================

const Value RasterRow::EMPTY_VALUE = Value();

bool FilterEval::Eval(const RasterRow &row, const FilterContext &ctx) {
	// If there are no filter expressions, the row passes the filter by default.
	if (ctx.expressions.empty()) {
		return true;
	}

	const auto &column_ids = ctx.column_ids;
	const auto &column_types = ctx.column_types;

	// Build a 1-row DataChunk with the projected columns populated from the RasterRow.
	vector<LogicalType> data_types;
	for (idx_t i = 0; i < column_ids.size(); i++) {
		const column_t table_col = column_ids[i];

		if (table_col >= RASTER_FIRST_BAND_COLUMN_INDEX) {
			data_types.emplace_back(LogicalType::BLOB);
		} else {
			data_types.emplace_back(column_types[table_col]);
		}
	}

	// Initialize the input chunk with the column values from the RasterRow.
	DataChunk input;
	input.Initialize(ctx.context, data_types, 1);

	for (idx_t i = 0; i < column_ids.size(); i++) {
		const column_t table_col = column_ids[i];
		input.data[i].SetValue(0, row.ValueOf(table_col));
	}
	input.SetCardinality(1);

	// Prepare a chunk to hold the results of expression evaluation.
	DataChunk result;
	result.Initialize(ctx.context, vector<LogicalType>(ctx.expressions.size(), LogicalType::BOOLEAN), 1);

	// Evaluate the filter!
	try {
		ExpressionExecutor executor(ctx.context);

		for (const auto &expr : ctx.expressions) {
			executor.AddExpression(*expr);
		}

		executor.Execute(input, result);

		for (idx_t i = 0; i < ctx.expressions.size(); i++) {
			const Value val = result.GetValue(i, 0);

			if (val.IsNull() || !BooleanValue::Get(val)) {
				return false;
			}
		}
	} catch (const std::exception &ex) {
		RASTER_SCAN_DEBUG_LOG(1, "Eval: exception during expression execution, Exception: %s", ex.what());

		// Defer to runtime evaluation on exception
		return true;
	}
	return true;
}

} // namespace duckdb
