#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;

// This is used to recursively skip projections
shared_ptr<OperatorLineage> GetThisLineageOp(PhysicalOperator *op, int thd_id) {
	switch (op->type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::DELIM_JOIN: {
		return op->lineage_op[thd_id];
	}
	case PhysicalOperatorType::HASH_JOIN: {
		JoinType join_type = dynamic_cast<PhysicalJoin *>(op)->join_type;
		if (join_type == JoinType::MARK) {
			// Pass through Mark Joins
			return GetThisLineageOp(op->children[0].get(), thd_id);
		} else {
			return op->lineage_op[thd_id];
		}
	}
	case PhysicalOperatorType::PROJECTION: {
		// Skip projection!
		return GetThisLineageOp(op->children[0].get(), thd_id);
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

std::vector<shared_ptr<OperatorLineage>> GetChildrenForOp(PhysicalOperator *op, int thd_id) {
	switch (op->type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		return {};
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW: {
		if (op->children.empty()) {
			// The aggregation in DelimJoin TODO figure this out
			return {};
		} else {
			return {GetThisLineageOp(op->children[0].get(), thd_id)};
		}
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {
		return {
			GetThisLineageOp(op->children[0].get(), thd_id),
			GetThisLineageOp(op->children[1].get(), thd_id)
		};

	}
	case PhysicalOperatorType::HASH_JOIN: {
		// Hash joins are all flipped around
		return {
		    GetThisLineageOp(op->children[1].get(), thd_id),
		    GetThisLineageOp(op->children[0].get(), thd_id)
		};
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this more deeply - will probably require re-arranging some child pointers
		PhysicalDelimJoin *delim_join = dynamic_cast<PhysicalDelimJoin *>(op);
		return {
		    GetThisLineageOp(op->children[0].get(), thd_id),
			GetThisLineageOp(delim_join->join.get(), thd_id),
			GetThisLineageOp((PhysicalOperator *)delim_join->distinct.get(), thd_id),
			// TODO DelimScans...
		};
	}
	case PhysicalOperatorType::PROJECTION: {
		// Set projection if it's the root (others will be skipped!)
		return {GetThisLineageOp(op->children[0].get(), thd_id)};
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

void LineageManager::CreateOperatorLineage(PhysicalOperator *op, int thd_id, bool trace_lineage, bool should_index) {
	should_index = should_index
	               && op->type != PhysicalOperatorType::ORDER_BY; // Order by is one chunk, so no need to index
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		auto distinct = (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		CreateOperatorLineage( distinct, thd_id, trace_lineage, true);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]->lineage_op  = distinct->lineage_op;
		}
		CreateOperatorLineage( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), thd_id, trace_lineage, true);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		bool child_should_index =
		    op->type == PhysicalOperatorType::HASH_GROUP_BY
		    || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		    || (op->type == PhysicalOperatorType::HASH_JOIN && i == 1) // Only build side child needs an index
		    || (op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::CROSS_PRODUCT && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::NESTED_LOOP_JOIN && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN && i == 1) // Right side needs index
		    || op->type == PhysicalOperatorType::ORDER_BY
		    || (op->type == PhysicalOperatorType::DELIM_JOIN && i == 0) // Child zero needs an index
		    || (op->type == PhysicalOperatorType::PROJECTION && should_index); // Pass through should_index on projection
		CreateOperatorLineage(op->children[i].get(), thd_id, trace_lineage, child_should_index);
	}
	
  if (op->type ==  PhysicalOperatorType::FILTER) {
    op->lineage_op[thd_id] = make_shared<FilterLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type ==  PhysicalOperatorType::LIMIT) {
    op->lineage_op[thd_id] = make_shared<LimitLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type ==  PhysicalOperatorType::ORDER_BY) {
    op->lineage_op[thd_id] = make_shared<OrderByLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::HASH_JOIN) {
    op->lineage_op[thd_id] = make_shared<HashJoinLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::INDEX_JOIN) {
    op->lineage_op[thd_id] = make_shared<IndexJoinLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::CROSS_PRODUCT) {
    op->lineage_op[thd_id] = make_shared<CrossLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::NESTED_LOOP_JOIN) {
    op->lineage_op[thd_id] = make_shared<NLJLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN) {
    op->lineage_op[thd_id] = make_shared<BNLJLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN) {
    op->lineage_op[thd_id] = make_shared<MergeLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
    op->lineage_op[thd_id] = make_shared<PHALineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::HASH_GROUP_BY) {
    op->lineage_op[thd_id] = make_shared<HALineage>(
        op->type,
        op->id,
        thd_id
    );
  } else if (op->type == PhysicalOperatorType::TABLE_SCAN) {
    op->lineage_op[thd_id] = make_shared<TableScanLineage>(
        op->type,
        op->id,
        thd_id
    );
  } else {
    op->lineage_op[thd_id] = make_shared<OperatorLineage>(
        GetChildrenForOp(op, thd_id),
        op->type,
        op->id,
        should_index
    );
  }

	op->lineage_op[thd_id]->trace_lineage = trace_lineage;
	if (
	    op->type == PhysicalOperatorType::HASH_JOIN ||
	    op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN ||
	    op->type == PhysicalOperatorType::NESTED_LOOP_JOIN ||
	    op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	) {
		// Cache join type so we can avoid anti joins
		op->lineage_op[thd_id]->join_type = dynamic_cast<PhysicalJoin *>(op)->join_type;
	}
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
idx_t PlanAnnotator(PhysicalOperator *op, idx_t counter) {
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter);
		counter = PlanAnnotator( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i)
			counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i], counter);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}
	op->id = counter;
	return counter + 1;
}

void LineageManager::InitOperatorPlan(PhysicalOperator *op) {
	PlanAnnotator(op, 0);
	CreateOperatorLineage(op, -1, trace_lineage, true); // Always index root
}

void LineageManager::StoreQueryLineage(std::unique_ptr<PhysicalOperator> op, string query) {
	if (!trace_lineage) return;
	// id of a query is their offset in query_to_id vector
	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	CreateLineageTables(op.get(), query_id);
	queryid_to_plan[query_id] = move(op);
}

} // namespace duckdb
#endif
