//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/projection/physical_projection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalProjection : public PhysicalOperator {
public:
	PhysicalProjection(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
	                   idx_t estimated_cardinality)
	    : PhysicalOperator(PhysicalOperatorType::PROJECTION, move(types), estimated_cardinality),
	      select_list(move(select_list)) {
#ifdef LINEAGE
		hasFunction = false;
		// check expressions, if any is a function then persist
		for (auto &expr : this->select_list) {
			if (expr->type != ExpressionType::BOUND_REF &&
			    expr->type != ExpressionType::COLUMN_REF &&
			    expr->type != ExpressionType::BOUND_COLUMN_REF) {
				hasFunction = true;
			}
		}
#endif
	}

	vector<unique_ptr<Expression>> select_list;
#ifdef LINEAGE
	bool hasFunction;
#endif
public:
	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	void FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) override;

	string ParamsToString() const override;
};

} // namespace duckdb
