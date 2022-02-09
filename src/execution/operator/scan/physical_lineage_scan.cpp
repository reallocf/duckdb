#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <utility>

namespace duckdb {

class PhysicalLineageScanOperatorState : public PhysicalOperatorState {
public:
	explicit PhysicalLineageScanOperatorState(PhysicalOperator &op)
	    : PhysicalOperatorState(op, nullptr), initialized(false) {
	}

	ParallelState *parallel_state;
	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
};

PhysicalLineageScan::PhysicalLineageScan(vector<LogicalType> types)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, move(types), 0) {
}

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto &state = (PhysicalLineageScanOperatorState &)*state_p;
	if (column_ids.empty()) {
		return;
	}
#ifdef LINEAGE
	context.setCurrent(this);
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
		function.function(context, bind_data.get(), state.operator_data.get(), nullptr, chunk);
		if (chunk.size() != 0) {
			return;
		}
	} else {
		// parallel scan
		do {
			function.function(context, bind_data.get(), state.operator_data.get(), nullptr, chunk);
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

string PhysicalLineageScan::GetName() const {
	return StringUtil::Upper(function.name);
}

string PhysicalLineageScan::ParamsToString() const {
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
			for (auto &filter : f.second) {
				if (filter.column_index < names.size()) {
					result += "\n";
					result += names[column_ids[filter.column_index]] +
					          ExpressionTypeToOperator(filter.comparison_type) + filter.constant.ToString();
				}
			}
		}
	}
	return result;
}

unique_ptr<PhysicalOperatorState> PhysicalLineageScan::GetOperatorState() {
	return make_unique<PhysicalLineageScanOperatorState>(*this);
}

} // namespace duckdb
