//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_query.hpp
//
//
//===----------------------------------------------------------------------===//
#ifdef LINEAGE
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> GenerateLineageQueryPlan(
    PhysicalOperator* op,
    ClientContext &cxt,
    ChunkCollection* lineage_ids,
    const string& mode,
    bool should_count = false,
    const string& input_table_name = ""
);

unique_ptr<PhysicalOperator> BuildLineagePipeline(
    PhysicalOperator* op,
    ClientContext &cxt,
    ChunkCollection* lineage_ids,
    unique_ptr<PhysicalOperator> left,
    bool simple_agg_flag,
    vector<unique_ptr<PhysicalOperator>> *pipelines
);

unique_ptr<PhysicalOperator> SwapRelationalLineageTablesForLineageQueryPlans(unique_ptr<PhysicalOperator> op, ClientContext &cxt);

template <typename T>
void Reverse(vector<unique_ptr<T>> *vec) {
	for (idx_t i = 0; i < vec->size() / 2; i++) {
		unique_ptr<T> tmp = move(vec->at(i));
		vec->at(i) = move(vec->at(vec->size() - i - 1));
		vec->at(vec->size() - i - 1) = move(tmp);
	}
}

unique_ptr<PhysicalOperator> CombineByMode(
    ClientContext &context,
    const string& mode,
    bool should_count,
    const string& input_table_name,
    unique_ptr<PhysicalOperator> first_plan,
    vector<unique_ptr<PhysicalOperator>> other_plans
);

vector<string> GetLineageTableNames(PhysicalOperator *op);
idx_t GetLineageOpSize(OperatorLineage *op);

} // namespace duckdb
#endif
