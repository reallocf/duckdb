//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_query.hpp
//
//
//===----------------------------------------------------------------------===//
#ifdef LINEAGE

namespace duckdb {

unique_ptr<PhysicalOperator> GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	bool simple_agg_flag,
	vector<unique_ptr<PhysicalOperator>> *pipelines);


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
    unique_ptr<PhysicalOperator> first_plan,
    vector<unique_ptr<PhysicalOperator>> other_plans
    );

} // namespace duckdb
#endif
