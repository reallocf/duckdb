#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"

#include <utility>

namespace duckdb {

class PhysicalLineageTableScanOperatorState : public PhysicalOperatorState {
public:
	explicit PhysicalLineageTableScanOperatorState(PhysicalOperator &op)
	    : PhysicalOperatorState(op, nullptr), initialized(false) {
	}

	ParallelState *parallel_state;
	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
	std::shared_ptr<LineageProcessStruct> lineageProcessStruct;
};


PhysicalLineageScan::PhysicalLineageScan(vector<LogicalType> types, TableFunction function_p,
                                         unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)) {
	TableScanBindData* tbldata = dynamic_cast<TableScanBindData *>(bind_data.get());
	DataTable* tbl = tbldata->table->storage.get();
	shared_ptr<DataTableInfo> info = tbl->info;
	opLineage = tbldata->table->opLineage;
	string st = info->table.substr(info->table.length()-1);
	finished_idx = stoi(st);
}

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalLineageTableScanOperatorState &)*state_p;
	TableScanBindData* tbldata = dynamic_cast<TableScanBindData*>(bind_data.get());
	auto types = tbldata->table->GetTypes();

	DataChunk base_chunk;
	base_chunk.Initialize(types);

	if (state.lineageProcessStruct == nullptr) {
		LineageProcessStruct lps = opLineage->Process(types, 0, base_chunk, 0, -1, 0, finished_idx);
		state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
	} else {
		LineageProcessStruct lps = opLineage->Process(types, state.lineageProcessStruct->count_so_far, base_chunk, state.lineageProcessStruct->size_so_far, -1, state.lineageProcessStruct->data_idx, state.lineageProcessStruct->finished_idx);
		state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
	}

	// Apply projection list
	chunk.SetCardinality(base_chunk.size());
	for (uint i=0; i < column_ids.size(); ++i) {
		chunk.data[i].Reference(base_chunk.data[column_ids[i]]);
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
