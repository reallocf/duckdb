#include "duckdb/execution/operator/join/physical_join.hpp"

namespace duckdb {

PhysicalJoin::PhysicalJoin(LogicalOperator &op, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : PhysicalJoin(op.types, type, join_type, estimated_cardinality) {
}

PhysicalJoin::PhysicalJoin(vector<LogicalType> types, PhysicalOperatorType type, JoinType join_type,
                           idx_t estimated_cardinality)
    : PhysicalSink(type, move(types), estimated_cardinality), join_type(join_type) {
}

} // namespace duckdb
