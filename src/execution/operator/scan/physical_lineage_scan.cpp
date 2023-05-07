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
	    : PhysicalOperatorState(op, nullptr), initialized(false), chunk_index(0), count_so_far(0) {
	}

	ParallelState *parallel_state;
	unique_ptr<FunctionOperatorData> operator_data;
	//! Whether or not the scan has been initialized
	bool initialized;
	std::shared_ptr<LineageProcessStruct> lineageProcessStruct;
	idx_t chunk_index;
	idx_t count_so_far;
};


PhysicalLineageScan::PhysicalLineageScan(ClientContext &context, shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types, TableFunction function_p,
                                         unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LINEAGE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)), lineage_op(lineage_op), base_tbl(nullptr) {
	TableScanBindData* tbldata = dynamic_cast<TableScanBindData *>(bind_data.get());
	string st = tbldata->table->name.substr(tbldata->table->name.length()-1);
	stage_idx = stoi(st);

	if (context.lineage_manager->base_tables.find(tbldata->table->name) != context.lineage_manager->base_tables.end()) {
		auto &phy_tbl_scan = (PhysicalTableScan &)*context.lineage_manager->base_tables[tbldata->table->name];
		base_tbl = ((TableScanBindData &)*phy_tbl_scan.bind_data).table;
	}
}

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalLineageTableScanOperatorState &)*state_p;
	TableScanBindData* tbldata = dynamic_cast<TableScanBindData*>(bind_data.get());
	auto lineage_table_types = tbldata->table->GetTypes();
	idx_t start = 0;
	    // base_table.types() = table.types() - lineage_table.types()
	DataChunk result;
	result.Initialize(lineage_table_types);

	// else if projection and chunk_collection is not empty, return everything in chunk_collection
	if (lineage_op->type == PhysicalOperatorType::PROJECTION) {
		start = state.count_so_far;
		if (lineage_op->chunk_collection.Count() == 0) {
			return;
		}
		D_ASSERT(result.GetTypes() == lineage_op->chunk_collection.Types());
		if (state.chunk_index >= lineage_op->chunk_collection.ChunkCount()) {
			return;
		}
		auto &collection_chunk = lineage_op->chunk_collection.GetChunk(state.chunk_index);
		result.Reference(collection_chunk);
		state.chunk_index++;
		state.count_so_far += result.size();
	} else {
		idx_t start = state.lineageProcessStruct == nullptr ? 0 : state.lineageProcessStruct->count_so_far;
		if (state.lineageProcessStruct == nullptr) {
			LineageProcessStruct lps = lineage_op->Process(lineage_table_types, 0, result, 0, -1, 0, stage_idx);
			state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
		} else {
			LineageProcessStruct lps = lineage_op->Process(lineage_table_types, state.lineageProcessStruct->count_so_far, result, state.lineageProcessStruct->size_so_far, -1, state.lineageProcessStruct->data_idx, state.lineageProcessStruct->finished_idx);
			state.lineageProcessStruct = std::make_shared<LineageProcessStruct>(lps);
		}
	}

	if (base_tbl && result.size() > 0) {
		idx_t lineage_table_offset = types.size() - base_tbl->GetTypes().size();
 		ColumnFetchState fetch_state;
		auto &transaction = Transaction::GetTransaction(context.client);
		DataChunk base_table_chunk;
		base_table_chunk.Initialize(base_tbl->GetTypes());

		// I need to get in_index column
		idx_t fetch_count = result.size();
		vector<column_t> fetch_ids;
		for (column_t i = 0; i < base_tbl->GetTypes().size(); ++i) {
			fetch_ids.push_back(i);
		}

		result.data[0].Normalify(result.size());
		vector<row_t> fetch_rows;
		for (idx_t i=0; i < result.size(); i++) {
			fetch_rows.push_back(result.data[0].GetValue(i).GetValue<row_t>());
 		}
 		Vector row_ids(result.data[0].GetType(), (data_ptr_t)&fetch_rows[0]);

		base_tbl->storage.get()->Fetch(transaction, base_table_chunk, fetch_ids, row_ids, fetch_count, fetch_state);
		//std::cout << base_table_chunk.ToString() << std::endl;
		for (idx_t i=0; i < base_tbl->GetTypes().size(); i++) {
			result.data[i+lineage_table_offset].Reference(base_table_chunk.data[i]);
		}
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
	// fill in from base_table_chunk
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
