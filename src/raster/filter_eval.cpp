#include "raster_types.hpp"
#include "raster_utils.hpp"
#include "filter_eval.hpp"

// DuckDB
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

namespace duckdb {

//======================================================================================================================
// RasterRow
//======================================================================================================================

const Value RasterRow::EMPTY_VALUE = Value();

vector<LogicalType> RasterRow::ValueTypesOf(const vector<column_t> &column_ids,
                                            const vector<LogicalType> &column_types) const {
	vector<LogicalType> data_types;

	for (idx_t i = 0; i < column_ids.size(); i++) {
		const column_t table_col = column_ids[i];

		if (table_col >= RASTER_FIRST_BAND_COLUMN_INDEX) {
			data_types.emplace_back(LogicalType::BLOB);
		} else {
			data_types.emplace_back(column_types[table_col]);
		}
	}
	return data_types;
}

const Value &RasterRow::ValueOf(int column_index) const {
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

//======================================================================================================================
// CellRow
//======================================================================================================================

const Value CellRow::EMPTY_VALUE = Value();

vector<LogicalType> CellRow::ValueTypesOf(const vector<column_t> &column_ids,
                                          const vector<LogicalType> &column_types) const {
	vector<LogicalType> data_types;

	for (idx_t i = 0; i < column_ids.size(); i++) {
		const column_t table_col = column_ids[i];

		if (table_col >= CELL_FIRST_BAND_COLUMN_INDEX) {
			data_types.emplace_back(LogicalType::DOUBLE);
		} else {
			data_types.emplace_back(column_types[table_col]);
		}
	}
	return data_types;
}

const Value &CellRow::ValueOf(int column_index) const {
	switch (column_index) {
	case CELL_ROWID_COLUMN_INDEX:
		return row_id;
	case CELL_X_COLUMN_INDEX:
		return x;
	case CELL_Y_COLUMN_INDEX:
		return y;
	case CELL_GEOMETRY_COLUMN_INDEX:
		return geometry;
	case CELL_COL_COLUMN_INDEX:
		return col;
	case CELL_ROW_COLUMN_INDEX:
		return row;
	default:
		return EMPTY_VALUE;
	}
}

//======================================================================================================================
// FilterEval
//======================================================================================================================

ExpressionExecutor *FilterEval::GetExecutor(const IFilterRow &row) {
	if (!executor) {
		vector<LogicalType> data_types = row.ValueTypesOf(column_ids, column_types);

		// Build a 1-row DataChunk with the projected columns populated from the ItemRow, and
		// initialize the input chunk with the column values from the rows.
		input.Initialize(context, data_types, 1);
		input.SetChildCardinality(1);

		// Prepare a chunk to hold the results of expression evaluation.
		result.Initialize(context, vector<LogicalType>(expressions.size(), LogicalType::BOOLEAN), 1);

		// Create the expression executor and add the filter expressions to it.

		executor = make_uniq<ExpressionExecutor>(context);

		for (const auto &expr : expressions) {
			executor->AddExpression(*expr);
		}
	}
	return executor.get();
}

bool FilterEval::Eval(const IFilterRow &row) {
	// If there are no filter expressions, the row passes the filter by default.
	if (expressions.empty()) {
		return true;
	}

	// Evaluate the filter!
	try {
		ExpressionExecutor *executor = GetExecutor(row);

		for (idx_t i = 0; i < column_ids.size(); i++) {
			const column_t table_col = column_ids[i];
			input.data[i].SetValue(0, row.ValueOf(table_col));
		}

		executor->Execute(input, result);

		for (idx_t i = 0; i < expressions.size(); i++) {
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
