#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/scan/physical_lineage_scan.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/common/enums/index_type.hpp"

#include <iostream>
#include <utility>

namespace duckdb {

class PhysicalIndexJoinOperatorState : public PhysicalOperatorState {
public:
	PhysicalIndexJoinOperatorState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right)
	    : PhysicalOperatorState(op, left) {
		D_ASSERT(left && right);
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			rhs_rows.emplace_back();
			result_sizes.emplace_back();
		}
	}

	idx_t lhs_idx = 0;
	idx_t rhs_idx = 0;
	idx_t result_size = 0;
	vector<idx_t> result_sizes;
	DataChunk join_keys;
	//! Vector of rows that mush be fetched for every LHS key
	vector<vector<row_t>> rhs_rows;
	ExpressionExecutor probe_executor;
	vector<shared_ptr<LineageDataWithOffset>> child_ptrs;
	vector<Vector> cached_values_arr;
	vector<vector<shared_ptr<LineageDataWithOffset>>> cached_child_ptrs_arr;
	idx_t overflow_count = 0;
	idx_t cached_values_idx = 0;
};

PhysicalIndexJoin::PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                     unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                     const vector<idx_t> &left_projection_map, vector<idx_t> right_projection_map,
                                     vector<column_t> column_ids_p, Index *index, bool lhs_first,
                                     idx_t estimated_cardinality, bool isLineageJoin, shared_ptr<OperatorLineage> operatorLineage,
                                     ChunkCollection *chunk_collection)
    : PhysicalOperator(PhysicalOperatorType::INDEX_JOIN, move(op.types), estimated_cardinality),
      left_projection_map(left_projection_map), right_projection_map(move(right_projection_map)), index(index),
      conditions(move(cond)), join_type(join_type), chunk_collection(chunk_collection), lhs_first(lhs_first) {
	column_ids = move(column_ids_p);
	children.push_back(move(left));
	children.push_back(move(right));
	this->isLineageJoin = isLineageJoin;
	this->opLineage = move(operatorLineage);
	for (auto &condition : conditions) {
		if (condition.left != nullptr) {
			condition_types.push_back(condition.left->return_type);
		}
	}
	//! Only add to fetch_ids columns that are not indexed
	for (auto &index_id : index->column_ids) {
		index_ids.insert(index_id);
	}
	for (idx_t column_id = 0; column_id < column_ids.size(); column_id++) {
		auto it = index_ids.find(column_ids[column_id]);
		if (it == index_ids.end()) {
			fetch_ids.push_back(column_ids[column_id]);
			fetch_types.push_back(children[1]->types[column_id]);
		}
	}
}

void PhysicalIndexJoin::Output(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p) const {
	auto &transaction = Transaction::GetTransaction(context.client);
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_p);
	if(!isLineageJoin) {
		auto &phy_tbl_scan = (PhysicalTableScan &)*children[1];
		auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
		auto tbl = bind_tbl.table->storage.get();
		DataChunk rhs_chunk;
		idx_t rhs_column_idx = 0;
		SelectionVector sel;
		sel.Initialize(STANDARD_VECTOR_SIZE);
		idx_t output_sel_idx = 0;
		vector<row_t> fetch_rows;
#ifdef LINEAGE
		bool do_not_fetch = false;
#endif
		auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_p);
		while (output_sel_idx < STANDARD_VECTOR_SIZE && state->lhs_idx < state->child_chunk.size()) {
			if (state->rhs_idx < state->result_sizes[state->lhs_idx]) {
				sel.set_index(output_sel_idx++, state->lhs_idx);
#ifdef LINEAGE
				if (!fetch_types.empty() || context.client.trace_lineage) {
					if (fetch_types.empty()) {
						do_not_fetch = true;
					}
#else
				if (!fetch_types.empty()) {
#endif
					//! We need to collect the rows we want to fetch
					fetch_rows.push_back(state->rhs_rows[state->lhs_idx][state->rhs_idx]);
				}
				state->rhs_idx++;
			} else {
				//! We are done with the matches from this LHS Key
				state->rhs_idx = 0;
				state->lhs_idx++;
			}
		}

		//! Now we fetch the RHS data
#ifdef LINEAGE
		if (!fetch_types.empty() && !do_not_fetch) {
#else
		if (!fetch_types.empty()) {
#endif
			if (fetch_rows.empty()) {
				return;
			}
			rhs_chunk.Initialize(fetch_types);
			ColumnFetchState fetch_state;
			Vector row_ids(LOGICAL_ROW_TYPE, (data_ptr_t)&fetch_rows[0]);
			tbl->Fetch(transaction, rhs_chunk, fetch_ids, row_ids, output_sel_idx, fetch_state);
		}

		//! Now we actually produce our result chunk
		idx_t left_offset = lhs_first ? 0 : right_projection_map.size();
		idx_t right_offset = lhs_first ? left_projection_map.size() : 0;
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			auto it = index_ids.find(column_ids[right_projection_map[i]]);
			if (it == index_ids.end()) {
				chunk.data[right_offset + i].Reference(rhs_chunk.data[rhs_column_idx++]);
			} else {
				chunk.data[right_offset + i].Reference(state->join_keys.data[0]);
				chunk.data[right_offset + i].Slice(sel, output_sel_idx);
			}
		}
		for (idx_t i = 0; i < left_projection_map.size(); i++) {
			chunk.data[left_offset + i].Reference(state->child_chunk.data[left_projection_map[i]]);
			chunk.data[left_offset + i].Slice(sel, output_sel_idx);
		}

#ifdef LINEAGE
		auto lhs_lineage = make_unique<LineageSelVec>(move(sel), output_sel_idx);
		auto rhs_lineage = make_unique<LineageDataRowVector>(fetch_rows, output_sel_idx);
		lineage_op.at(context.task.thread_id)
		    ->Capture(make_shared<LineageBinary>(move(lhs_lineage), move(rhs_lineage)), LINEAGE_UNARY);
#endif

		state->result_size = output_sel_idx;
		chunk.SetCardinality(state->result_size);
	}
	else {
		DataChunk join_chunk;
		vector<shared_ptr<LineageDataWithOffset>> child_ptrs;

		// 1. Access THIS child_ptrs to pass to AccessIndex
		if (dynamic_cast<PhysicalIndexJoinOperatorState*>(state->child_state.get()) != nullptr){
			auto child_state = dynamic_cast<PhysicalIndexJoinOperatorState*>(state->child_state.get());
			if (!child_state->child_ptrs.empty()) {
				child_ptrs = child_state->child_ptrs;
			}
		}
		if (child_ptrs.empty()) {
			child_ptrs.reserve(STANDARD_VECTOR_SIZE);
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				child_ptrs.push_back(nullptr);
			}
		}

		bool was_set = state->child_chunk.lineage_agg_data.empty();
		opLineage->AccessIndex({state->child_chunk, child_ptrs, join_chunk, state->cached_values_arr, state->cached_child_ptrs_arr, state->overflow_count});
		bool is_set = state->child_chunk.lineage_agg_data.empty();

		// 2. Set PARENT'S child_ptrs so that it can pass it to AccessIndex
		state->child_ptrs = child_ptrs;
		chunk.Reference(state->child_chunk); // TODO should this be a chunk.Move()?
		if (!was_set && is_set) {
			chunk.lineage_agg_data = move(state->child_chunk.lineage_agg_data); // TODO: is moving right?
			chunk.outer_agg_idx = state->child_chunk.outer_agg_idx;
			chunk.inner_agg_idx = state->child_chunk.inner_agg_idx;
			state->child_chunk.lineage_agg_data = vector<shared_ptr<vector<SourceAndMaybeData>>>();
			state->child_chunk.outer_agg_idx = 0;
			state->child_chunk.inner_agg_idx = 0;
		}
		state->result_size = state->child_chunk.size();
		state->lhs_idx += state->child_chunk.size();
		if (join_chunk.size() > 0) {
			chunk_collection->Append(join_chunk);
		}
	}
}

void PhysicalIndexJoin::GetRHSMatches(ExecutionContext &context, PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_p);
	if (!isLineageJoin){
		auto &art = (ART &)*index;
		auto &transaction = Transaction::GetTransaction(context.client);
		for (idx_t i = 0; i < state->child_chunk.size(); i++) {
			auto equal_value = state->join_keys.GetValue(0, i);
			auto index_state = art.InitializeScanSinglePredicate(transaction, equal_value, ExpressionType::COMPARE_EQUAL);
			state->rhs_rows[i].clear();
			if (!equal_value.is_null) {
#ifdef LINEAGE
				if (fetch_types.empty() && !context.client.trace_lineage) {
#else
				if (fetch_types.empty()) {
#endif
					IndexLock lock;
					index->InitializeLock(lock);
					art.SearchEqualJoinNoFetch(equal_value, state->result_sizes[i]);
				} else {
					IndexLock lock;
					index->InitializeLock(lock);
					art.SearchEqual((ARTIndexScanState *)index_state.get(), (idx_t)-1, state->rhs_rows[i]);
					state->result_sizes[i] = state->rhs_rows[i].size();
				}
			} else {
				//! This is null so no matches
				state->result_sizes[i] = 0;
			}
		}
		for (idx_t i = state->child_chunk.size(); i < STANDARD_VECTOR_SIZE; i++) {
			//! No LHS chunk value so result size is empty
			state->result_sizes[i] = 0;
		}
	}
}

void PhysicalIndexJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                         PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalIndexJoinOperatorState *>(state_p);
	state->result_size = 0;
	while (state->result_size == 0) {
		// Fancy lineage cache management
		if (state->child_chunk.lineage_agg_data.size() > state->child_chunk.outer_agg_idx) {
			Output(context, chunk, state_p);
			return;
		}
		// Return cached values if there are any
		if (!state->cached_values_arr.empty()) {
			chunk.data[0].Reference(state->cached_values_arr[state->cached_values_idx]);
			if (!state->cached_child_ptrs_arr.empty()) {
				state->child_ptrs = state->cached_child_ptrs_arr[state->cached_values_idx];
			}
			if (state->overflow_count > STANDARD_VECTOR_SIZE) {
				chunk.SetCardinality(STANDARD_VECTOR_SIZE);
				state->overflow_count -= STANDARD_VECTOR_SIZE;
			} else {
				chunk.SetCardinality(state->overflow_count);
				state->overflow_count = 0;
			}
			state->cached_values_idx++;
			if (state->cached_values_idx == state->cached_values_arr.size()) {
				D_ASSERT(state->overflow_count == 0);
				state->cached_values_idx = 0;
				state->cached_values_arr.clear();
				state->cached_child_ptrs_arr.clear();
			}
			return;
		}
		//! Check if we need to get a new LHS chunk
		if (state->lhs_idx >= state->child_chunk.size()) {
			children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
			if (state->child_chunk.size() == 0) {
				//! If chunk is empty there is nothing else to probe
				chunk.SetCardinality(state->result_size);
				return;
			}
			state->lhs_idx = 0;
			state->rhs_idx = 0;
			state->probe_executor.Execute(state->child_chunk, state->join_keys);
		}
		//! Fill Matches for the current LHS chunk
		if (state->lhs_idx == 0 && state->rhs_idx == 0) {
			GetRHSMatches(context, state_p);
		}
		//! Output vectors
		if (state->lhs_idx < state->child_chunk.size()) {
			Output(context, chunk, state_p);
		}
	}
}

unique_ptr<PhysicalOperatorState> PhysicalIndexJoin::GetOperatorState() {
	auto state = make_unique<PhysicalIndexJoinOperatorState>(*this, children[0].get(), children[1].get());
	if (right_projection_map.empty()) {
		for (column_t i = 0; i < column_ids.size(); i++) {
			right_projection_map.push_back(i);
		}
	}
	if (left_projection_map.empty()) {
		for (column_t i = 0; i < state->child_chunk.ColumnCount(); i++) {
			left_projection_map.push_back(i);
		}
	}
	if(condition_types.size() == 0){
		condition_types.push_back(LogicalType::INTEGER);
	}
	state->join_keys.Initialize(condition_types);
	for (auto &cond : conditions) {
		if(cond.left){
			state->probe_executor.AddExpression(*cond.left);
		}
	}
	return move(state);
}
void PhysicalIndexJoin::FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) {
	auto &state_p = reinterpret_cast<PhysicalIndexJoinOperatorState &>(state);
	context.thread.profiler.Flush(this, &state_p.probe_executor, "probe_executor", 0);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}

} // namespace duckdb
