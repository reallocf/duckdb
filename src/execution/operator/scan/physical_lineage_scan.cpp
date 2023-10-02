#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/transaction/transaction.hpp"
 #include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/function/table/table_scan.hpp"
 #include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include <utility>

namespace duckdb {

class PhysicalTableScan;

class PhysicalLineageTableScanOperatorState : public PhysicalOperatorState {
public:
	explicit PhysicalLineageTableScanOperatorState(PhysicalOperator &op)
	    : PhysicalOperatorState(op, nullptr), initialized(false), chunk_index(0), count_so_far(0), thread_id(-1), thread_pos(0) {
	}

	ParallelState *parallel_state;
	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
	std::shared_ptr<LineageProcessStruct> lineageProcessStruct;
	idx_t chunk_index;
	idx_t count_so_far;
	int thread_id;
	idx_t thread_pos;
};


PhysicalLineageScan::PhysicalLineageScan(ClientContext &context, std::unordered_map<int, shared_ptr<OperatorLineage>> lineage_op, vector<LogicalType> types, TableFunction function_p,
                                         unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality, idx_t stage_idx)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)), stage_idx(stage_idx), lineage_op(lineage_op) {
	for(auto kv : lineage_op) {
		thread_id_list.push_back(kv.first);
	}
}

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalLineageTableScanOperatorState &)*state_p;
	TableScanBindData* tbldata = dynamic_cast<TableScanBindData*>(bind_data.get());
	auto lineage_table_types = tbldata->table->GetTypes();
	idx_t start = 0;
	DataChunk result;
	result.Initialize(lineage_table_types);

	if (state.thread_pos < thread_id_list.size())
		state.thread_id = thread_id_list[state.thread_pos];

	// else if projection and chunk_collection is not empty, return everything in chunk_collection
	if (stage_idx == 100) {
		start = state.count_so_far;
		if (lineage_op.at(state.thread_id)->chunk_collection.Count() == 0) {
			return;
		}
		D_ASSERT(result.GetTypes() == lineage_op.at(state.thread_id)->chunk_collection.Types());
		if (state.chunk_index >= lineage_op.at(state.thread_id)->chunk_collection.ChunkCount()) {
			return;
		}
		auto &collection_chunk = lineage_op.at(state.thread_id)->chunk_collection.GetChunk(state.chunk_index);
		result.Reference(collection_chunk);
		state.chunk_index++;
		state.count_so_far += result.size();
	} else {
		do {
			start = state.lineageProcessStruct == nullptr ? 0 : state.lineageProcessStruct->count_so_far;
			if (state.lineageProcessStruct == nullptr) {
				LineageProcessStruct lps =
				    lineage_op.at(state.thread_id)->GetLineageAsChunk(lineage_table_types, 0, result, 0, state.thread_id, 0, stage_idx);
				state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
			} else {
				LineageProcessStruct lps = lineage_op.at(state.thread_id)->GetLineageAsChunk(
				    lineage_table_types, state.lineageProcessStruct->count_so_far, result,
				    state.lineageProcessStruct->size_so_far, state.thread_id, state.lineageProcessStruct->data_idx,
				    state.lineageProcessStruct->finished_idx);
				state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
			}
			if (!state.lineageProcessStruct->still_processing) {
				if (state.thread_pos + 1 < thread_id_list.size())  {
					state.lineageProcessStruct->count_so_far = 0;
					state.thread_pos++;
					state.lineageProcessStruct->data_idx = 0;
					state.lineageProcessStruct->finished_idx = 0;
					state.lineageProcessStruct->still_processing = true;
					state.thread_id = thread_id_list[state.thread_pos];
				}
			}
		} while (result.size() == 0 && state.lineageProcessStruct->still_processing);
	}

	// Apply projection list
	chunk.SetCardinality(result.size());
	for (uint col_idx=0; col_idx < column_ids.size(); ++col_idx) {
		idx_t column = column_ids[col_idx];
		if (column == COLUMN_IDENTIFIER_ROW_ID) {
			// row id column: fill in the row ids
			D_ASSERT(chunk.data[col_idx].GetType().InternalType() == PhysicalType::INT64);
			chunk.data[col_idx].Sequence(start, 1);
		}  else {
			chunk.data[col_idx].Reference(result.data[column]);
		}
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
	return result;
}

unique_ptr<PhysicalOperatorState> PhysicalLineageScan::GetOperatorState() {
	return make_unique<PhysicalLineageTableScanOperatorState>(*this);
}



} // namespace duckdb
