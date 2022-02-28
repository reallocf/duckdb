#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/data_table.hpp"

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


PhysicalLineageScan::PhysicalLineageScan(vector<LogicalType> types, TableFunction function_p,
                                         unique_ptr<FunctionData> bind_data_p, vector<column_t> column_ids_p,
                                         vector<string> names_p, unique_ptr<TableFilterSet> table_filters_p,
                                         idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::TABLE_SCAN, move(types), estimated_cardinality),
      function(move(function_p)), bind_data(move(bind_data_p)), column_ids(move(column_ids_p)), names(move(names_p)),
      table_filters(move(table_filters_p)) {
}

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &state = (PhysicalTableScanOperatorState &)*state_p;

	TableScanBindData* tbldata = dynamic_cast<TableScanBindData *>(this->bind_data.get());
	DataTable* tbl = tbldata->table->storage.get();
	shared_ptr<DataTableInfo> info = tbl->info;
	TableCatalogEntry * table = Catalog::GetCatalog(context.client).GetEntry<TableCatalogEntry>(context.client,  DEFAULT_SCHEMA, info->table);
	DataChunk insert_chunk;
	insert_chunk.Initialize(table->GetTypes());
	// set the cardinality after reading from the data structure?
	insert_chunk.SetCardinality(1);

	TableFilterCollection filters(table_filters.get());
	TableFilterSet *filterSet = filters.table_filters;

	// Iterate through all the filters (unordered set idx VS (Constant, ExpresssionType, column_idx)) apply the relevant conditions with values on the column_idx


	// populate chunk
	chunk.SetValue(0,0,0);
	chunk.SetValue(0,1,1);
	chunk.SetValue(0,2,2);

	chunk.SetValue(1,0,0);
	chunk.SetValue(1,1,1);
	chunk.SetValue(1,2,1999);

	chunk.SetValue(2,0,0);
	chunk.SetValue(2,1,1);
	chunk.SetValue(2,2,2);

	chunk.SetCardinality(3);

	state.initialized = true;
	state.finished = true;

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

unique_ptr<PhysicalOperatorState> PhysicalLineageScan::GetOperatorState() {
	return make_unique<PhysicalTableScanOperatorState>(*this);
}



} // namespace duckdb