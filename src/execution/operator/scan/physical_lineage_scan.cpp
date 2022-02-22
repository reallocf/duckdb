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

void PhysicalLineageScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) {
	auto &state = (PhysicalTableScanOperatorState &)*state_p;

	TableCatalogEntry * table = Catalog::GetCatalog(context.client).GetEntry<TableCatalogEntry>(context.client,  DEFAULT_SCHEMA, "filter_9_0");
	DataChunk insert_chunk;
	insert_chunk.Initialize(table->GetTypes());
	// set the cardinality after reading from the data structure?
	insert_chunk.SetCardinality(1);

	// row id
	Vector rowid;
	rowid.SetType(table->GetTypes()[0]);
	rowid.Reference( Value::BIGINT(0));


	// chunk_id
	Vector chunk_id;
	chunk_id.SetType(table->GetTypes()[1]);
	chunk_id.Reference( Value::INTEGER(0));

	Vector index;
	index.SetType(table->GetTypes()[2]);
	index.Reference( Value::INTEGER(0));


	// value
	Vector value;
	value.SetType(table->GetTypes()[3]);
	value.Reference( Value::INTEGER(0));

	// populate chunk
	chunk.data[0].Reference(rowid);
	chunk.data[1].Reference(chunk_id);
	chunk.data[2].Reference(index);
	chunk.data[3].Reference(value);

	chunk.SetCardinality(1);

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
	return make_unique<PhysicalTableScanOperatorState>(*this);
}



} // namespace duckdb
