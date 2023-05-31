#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DuckDBQueriesListData : public FunctionOperatorData {
	DuckDBQueriesListData() : offset(0) {
	}

	idx_t offset;
};

//! Create table to store executed queries with their IDs
//! Table name: duckdb_queries_list()
//! Schema: (INT query_id, VARCHAR query)
static unique_ptr<FunctionData> DuckDBQueriesListBind(ClientContext &context, vector<Value> &inputs,
                                                      unordered_map<string, Value> &named_parameters,
                                                      vector<LogicalType> &input_table_types,
                                                      vector<string> &input_table_names,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("query_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("query");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> DuckDBQueriesListInit(ClientContext &context, const FunctionData *bind_data,
                                                       const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto result = make_unique<DuckDBQueriesListData>();
	return std::move(result);
}

void DuckDBQueriesListFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                               DataChunk *input, DataChunk &output) {
	auto &data = (DuckDBQueriesListData &)*operator_state;
	auto queryid_to_plan = context.lineage_manager->query_to_id;
	if (data.offset >= queryid_to_plan.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < queryid_to_plan.size() && count < STANDARD_VECTOR_SIZE) {
		string query = queryid_to_plan[data.offset];
		idx_t col = 0;
		// query_id, INT
		output.SetValue(col++, count,Value::INTEGER(data.offset));
		// query, VARCHAR
		output.SetValue(col++, count, query);

		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

void DuckDBQueriesListFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_queries_list", {}, DuckDBQueriesListFunction, DuckDBQueriesListBind, DuckDBQueriesListInit));
}

} // namespace duckdb
