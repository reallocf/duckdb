#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id) {
	if (!trace_lineage || datum->Count() == 0) return;
	// Prepare this vector's chunk to be passed on to future operators
	pipeline_lineage->AdjustChunkOffsets(datum->Count(), lineage_idx);

	// Set child ptr
	datum->SetChild(GetChildLatest(lineage_idx));

	// Capture this vector
	idx_t child_offset = pipeline_lineage->GetChildChunkOffset(lineage_idx);
	idx_t this_offset = GetThisOffset(lineage_idx);
	if (lineage_idx == LINEAGE_COMBINE) {
		data[lineage_idx].push_back(LineageDataWithOffset{datum, thread_id, this_offset});
	} else {
		data[lineage_idx].push_back(LineageDataWithOffset{datum, (int)child_offset, this_offset});
	}
}

shared_ptr<PipelineLineage> OperatorLineage::GetPipelineLineage() {
	return pipeline_lineage;
}

void OperatorLineage::MarkChunkReturned() {
	dynamic_cast<PipelineJoinLineage *>(pipeline_lineage.get())->MarkChunkReturned();
}

void OperatorLineage::SetChunkId(idx_t idx) {
	dynamic_cast<PipelineScanLineage *>(pipeline_lineage.get())->SetChunkId(idx);
}

idx_t OperatorLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : data[0]) {
		size += lineage_data.data->Size();
	}
	for (const auto& lineage_data : data[1]) {
		size += lineage_data.data->Size();
	}
	return size;
}

shared_ptr<LineageDataWithOffset> OperatorLineage::GetMyLatest() {
	switch (type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		if (!data[0].empty()) {
			return make_shared<LineageDataWithOffset>(data[0][data[0].size() - 1]);
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Simple agg = ALL lineage, so child lineage data ptrs are meaningless
		return nullptr;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY: {
		if (!data[LINEAGE_UNARY].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY][data[LINEAGE_UNARY].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::WINDOW: {
		if (!data[LINEAGE_SOURCE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_SOURCE][data[LINEAGE_SOURCE].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::CROSS_PRODUCT: {
		// Only the right lineage is ever captured TODO is this what we should do?
		if (!data[LINEAGE_PROBE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {
		// 0 is the probe side for these joins
		if (!data[0].empty()) {
			return make_shared<LineageDataWithOffset>(data[0][data[0].size() - 1]);
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// 1 is the probe side for this join
		return make_shared<LineageDataWithOffset>(data[1][data[1].size() - 1]);
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// When being asked for latest, we'll always want to refer to the probe data
		if (!data[LINEAGE_PROBE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1]);
		} else {
			// Pass through child for Mark Hash Join TODO is this right?
			return children[LINEAGE_PROBE]->GetMyLatest();
		}
	}
	case PhysicalOperatorType::PROJECTION: {
		throw std::logic_error("We shouldn't ever try to call GetMyLatest on a Projection");
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this
		return {};
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

shared_ptr<LineageDataWithOffset> OperatorLineage::GetChildLatest(idx_t lineage_idx) {
	switch (type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::TABLE_SCAN: {
		return nullptr;
	}
	case PhysicalOperatorType::ORDER_BY: {
		return nullptr; // Order By has no children since ALL are its children
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// Only SINK has children
		if (children.empty()) {
			// The aggregation in DelimJoin TODO figure this out
			return nullptr;
		} else if (lineage_idx == LINEAGE_SINK) {
			return children[0]->GetMyLatest();
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW: {
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// Index Join, despite being a join, just has 1 child
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// These only capture 1 lineage on PROBE side and we really care about BUILD side child
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// We mix up Hash Join...
		if (lineage_idx == LINEAGE_BUILD) {
			return children[0]->GetMyLatest();
		} else {
			return children[1]->GetMyLatest();
		}
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this
		throw std::logic_error("Haven't handled delim join yet");
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

idx_t OperatorLineage::GetThisOffset(idx_t lineage_idx) {
	idx_t last_data_idx = data[lineage_idx].size() - 1;
	return data[lineage_idx].empty() ? 0 : data[lineage_idx][last_data_idx].this_offset + data[lineage_idx][last_data_idx].data->Count();
}

} // namespace duckdb
#endif
