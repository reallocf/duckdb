#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {
class PhysicalDelimJoin;

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

	names.emplace_back("size_bytes_max");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("size_bytes_min");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("nchunks");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("postprocess_time");
	return_types.emplace_back(LogicalType::FLOAT);

	return nullptr;
}

unique_ptr<FunctionOperatorData> DuckDBQueriesListInit(ClientContext &context, const FunctionData *bind_data,
                                                       const vector<column_t> &column_ids, TableFilterCollection *filters) {
	auto result = make_unique<DuckDBQueriesListData>();
	return std::move(result);
}

void PostProcess(PhysicalOperator *op, bool should_index) {
	// massage the data to make it easier to query
  // for hash join, build hash table on the build side that map the address to id
  // for group by, build hash table on the unique groups
  for (auto const& lineage_op : op->lineage_op) {
    lineage_op.second->BuildIndexes();
  }

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->children[0].get(), true);
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), true);
		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), true);
		return;
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		PostProcess(op->children[i].get(), should_index);
	}
}

std::vector<idx_t> GetStats(PhysicalOperator *op) {
  if (!op) {
    std::cout << "null" << std::endl;
  }
  // for each thread's log
  idx_t lineage_size = 0;
  idx_t chunks_processed = 0;
  idx_t count = 0;
  for (auto const& lineage_op : op->lineage_op) {
    lineage_size = lineage_op.second->Size();
    chunks_processed = lineage_op.second->data[0].size();
    count = lineage_op.second->Count();
  }

  std::vector<idx_t> stats(3, 0);
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		stats = GetStats( dynamic_cast<PhysicalDelimJoin *>(op)->children[0].get());
    lineage_size += stats[0];
    chunks_processed += stats[1];
    count += stats[2];
	  stats = GetStats( dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
    lineage_size += stats[0];
    chunks_processed += stats[1];
    count += stats[2];
		stats = GetStats( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get());
    lineage_size += stats[0];
    chunks_processed += stats[1];
    count += stats[2];

    stats[0] = lineage_size;
    stats[1] = chunks_processed;
    stats[2] = count;
		return stats;
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		stats = GetStats(op->children[i].get());
    lineage_size += stats[0];
    chunks_processed += stats[1];
    count += stats[2];
	}

  stats[0] = lineage_size;
  stats[1] = chunks_processed;
  stats[2] = count;

	return stats;
}

void DuckDBQueriesListFunction(ClientContext &context, const FunctionData *bind_data, FunctionOperatorData *operator_state,
                               DataChunk *input, DataChunk &output) {
	auto &data = (DuckDBQueriesListData &)*operator_state;
	auto queryid_to_query = context.lineage_manager->query_to_id;
	if (data.offset >= queryid_to_query.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < queryid_to_query.size() && count < STANDARD_VECTOR_SIZE) {
		string query = queryid_to_query[data.offset];
    std::cout << "id: " << data.offset << " " << query << std::endl;
    auto plan = context.lineage_manager->queryid_to_plan[data.offset].get();

    auto stats = GetStats(plan);
    clock_t start = clock();
    PostProcess(plan, true);
    clock_t end = clock();
		idx_t col = 0;
		// query_id, INT
		output.SetValue(col++, count,Value::INTEGER(data.offset));
		// query, VARCHAR
		output.SetValue(col++, count, query);

    // size_byes_max
		output.SetValue(col++, count,Value::INTEGER(stats[0]));

    // size_bytes_min
		output.SetValue(col++, count,Value::INTEGER(stats[2]));

    // nchunks
		output.SetValue(col++, count,Value::INTEGER(stats[1]));

    // postprocess_time
    float postprocess_time = ((float) end - start) / CLOCKS_PER_SEC;
		output.SetValue(col++, count,Value::FLOAT(postprocess_time));

		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

void DuckDBQueriesListFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_queries_list", {}, DuckDBQueriesListFunction, DuckDBQueriesListBind, DuckDBQueriesListInit));
}

} // namespace duckdb
