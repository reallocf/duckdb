#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

class PhysicalTableScanOperatorState : public PhysicalOperatorState {
public:
	explicit PhysicalTableScanOperatorState(PhysicalOperator &op)
	    : PhysicalOperatorState(op, nullptr), initialized(false) {
	}

	ParallelState *parallel_state;
	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
};

PhysicalTableScan::PhysicalTableScan(vector<LogicalType> types, TableFunction function_p,
                                     unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                     vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                     idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)) {
}

void PhysicalTableScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                         PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalTableScanOperatorState &)*state_p;
	if (column_ids.empty()) {
		return;
	}
#ifdef LINEAGE
    context.SetCurrentLineageOp(lineage_op);
#endif
	if (!state.initialized) {
		state.parallel_state = nullptr;
		if (function.init) {
			auto &task = context.task;
			// check if there is any parallel state to fetch
			state.parallel_state = nullptr;
			auto task_info = task.task_info.find(this);
			TableFilterCollection filters(table_filters.get());
			if (task_info != task.task_info.end()) {
				// parallel scan init
				state.parallel_state = task_info->second;
				state.operator_data =
				    function.parallel_init(context.client, bind_data.get(), state.parallel_state, column_ids, &filters);
			} else {
				// sequential scan init
				state.operator_data = function.init(context.client, bind_data.get(), column_ids, &filters);
			}
			if (!state.operator_data) {
				// no operator data returned: nothing to scan
				return;
			}
		}
		state.initialized = true;
	}
	if (!state.parallel_state) {
		// sequential scan
#ifdef LINEAGE
		function.function(context, bind_data.get(), state.operator_data.get(), nullptr, chunk);
#else
		function.function(context.client, bind_data.get(), state.operator_data.get(), nullptr, chunk);
#endif
		if (chunk.size() != 0) {
			return;
		}
	} else {
		// parallel scan
		do {
			if (function.parallel_function) {
#ifdef LINEAGE
				function.parallel_function(context, bind_data.get(), state.operator_data.get(), nullptr, chunk,
				                           state.parallel_state);
#else
				function.parallel_function(context.client, bind_data.get(), state.operator_data.get(), nullptr, chunk,
				                           state.parallel_state);
#endif
			} else {
#ifdef LINEAGE
				function.function(context, bind_data.get(), state.operator_data.get(), nullptr, chunk);
#else
				function.function(context.client, bind_data.get(), state.operator_data.get(), nullptr, chunk);
#endif
			}

			if (chunk.size() == 0) {
				D_ASSERT(function.parallel_state_next);
				if (function.parallel_state_next(context.client, bind_data.get(), state.operator_data.get(),
				                                 state.parallel_state)) {
					continue;
				} else {
					break;
				}
			} else {
				return;
			}
		} while (true);
	}
	D_ASSERT(chunk.size() == 0);
	if (function.cleanup) {
		function.cleanup(context.client, bind_data.get(), state.operator_data.get());
	}
}

string PhysicalTableScan::GetName() const {
	return StringUtil::Upper(function.name);
}

string PhysicalTableScan::ParamsToString() const {
	string result;
	if (function.to_string) {
		result = function.to_string(bind_data.get());
		result += "\n[INFOSEPARATOR]\n";
	}
	if (function.projection_pushdown) {
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i] < names.size()) {
				if (i > 0) {
					result += "\n";
				}
				result += names[column_ids[i]];
			}
		}
	}
	if (function.filter_pushdown && table_filters) {
		result += "\n[INFOSEPARATOR]\n";
		result += "Filters: ";
		for (auto &f : table_filters->filters) {
			auto &column_index = f.first;
			auto &filter = f.second;
			if (column_index < names.size()) {
				result += filter->ToString(names[column_ids[column_index]]);
				result += "\n";
			}
		}
	}
	return result;
}

unique_ptr<PhysicalOperatorState> PhysicalTableScan::GetOperatorState() {
	return make_unique<PhysicalTableScanOperatorState>(*this);
}

} // namespace duckdb
