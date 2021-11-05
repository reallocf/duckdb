#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

bool CanCacheType(const LogicalType &type);

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   const vector<idx_t> &left_projection_map,
                                   const vector<idx_t> &right_projection_map_p, vector<LogicalType> delim_types,
                                   idx_t estimated_cardinality)
    : PhysicalComparisonJoin(op, PhysicalOperatorType::HASH_JOIN, move(cond), join_type, estimated_cardinality),
      right_projection_map(right_projection_map_p), delim_types(move(delim_types)) {
	children.push_back(move(left));
	children.push_back(move(right));

	D_ASSERT(left_projection_map.size() == 0);
	for (auto &condition : conditions) {
		condition_types.push_back(condition.left->return_type);
	}

	// for ANTI, SEMI and MARK join, we only need to store the keys, so for these the build types are empty
	if (join_type != JoinType::ANTI && join_type != JoinType::SEMI && join_type != JoinType::MARK) {
		build_types = LogicalOperator::MapTypes(children[1]->GetTypes(), right_projection_map);
	}
	// we avoid caching lists, since lists can be very large caching can have very negative effects
	can_cache = true;
	for (auto &type : types) {
		if (!CanCacheType(type)) {
			can_cache = false;
		}
	}
}

PhysicalHashJoin::PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                   unique_ptr<PhysicalOperator> right, vector<JoinCondition> cond, JoinType join_type,
                                   idx_t estimated_cardinality)
    : PhysicalHashJoin(op, move(left), move(right), move(cond), join_type, {}, {}, {}, estimated_cardinality) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashJoinLocalState : public LocalSinkState {
public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;
};

class HashJoinGlobalState : public GlobalOperatorState {
public:
	HashJoinGlobalState() {
	}

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState ht_scan_state;
};

unique_ptr<GlobalOperatorState> PhysicalHashJoin::GetGlobalState(ClientContext &context) {
	auto state = make_unique<HashJoinGlobalState>();
	state->hash_table =
	    make_unique<JoinHashTable>(BufferManager::GetBufferManager(context), conditions, build_types, join_type);
	if (!delim_types.empty() && join_type == JoinType::MARK) {
		// correlated MARK join
		if (delim_types.size() + 1 == conditions.size()) {
			// the correlated MARK join has one more condition than the amount of correlated columns
			// this is the case in a correlated ANY() expression
			// in this case we need to keep track of additional entries, namely:
			// - (1) the total amount of elements per group
			// - (2) the amount of non-null elements per group
			// we need these to correctly deal with the cases of either:
			// - (1) the group being empty [in which case the result is always false, even if the comparison is NULL]
			// - (2) the group containing a NULL value [in which case FALSE becomes NULL]
			auto &info = state->hash_table->correlated_mark_join_info;

			vector<LogicalType> payload_types;
			vector<BoundAggregateExpression *> correlated_aggregates;
			unique_ptr<BoundAggregateExpression> aggr;

			// jury-rigging the GroupedAggregateHashTable
			// we need a count_star and a count to get counts with and without NULLs
			aggr = AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}, nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			auto count_fun = CountFun::GetFunction();
			vector<unique_ptr<Expression>> children;
			// this is a dummy but we need it to make the hash table understand whats going on
			children.push_back(make_unique_base<Expression, BoundReferenceExpression>(count_fun.return_type, 0));
			aggr = AggregateFunction::BindAggregateFunction(context, count_fun, move(children), nullptr, false);
			correlated_aggregates.push_back(&*aggr);
			payload_types.push_back(aggr->return_type);
			info.correlated_aggregates.push_back(move(aggr));

			info.correlated_counts = make_unique<GroupedAggregateHashTable>(
			    BufferManager::GetBufferManager(context), delim_types, payload_types, correlated_aggregates);
			info.correlated_types = delim_types;
			info.group_chunk.Initialize(delim_types);
			info.result_chunk.Initialize(payload_types);
		}
	}
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalHashJoin::GetLocalSinkState(ExecutionContext &context) {
	auto state = make_unique<HashJoinLocalState>();
	if (!right_projection_map.empty()) {
		state->build_chunk.Initialize(build_types);
	}
	for (auto &cond : conditions) {
		state->build_executor.AddExpression(*cond.right);
	}
	state->join_keys.Initialize(condition_types);
	return move(state);
}

void PhysicalHashJoin::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate_p,
                            DataChunk &input) const {
	auto &sink = (HashJoinGlobalState &)state;
	auto &lstate = (HashJoinLocalState &)lstate_p;
#ifdef LINEAGE
	context.SetCurrentLineageOp(lineage_op);
#endif
	// resolve the join keys for the right chunk
	lstate.build_executor.Execute(input, lstate.join_keys);
	// build the HT
	if (!right_projection_map.empty()) {
		// there is a projection map: fill the build chunk with the projected columns
		lstate.build_chunk.Reset();
		lstate.build_chunk.SetCardinality(input);
		for (idx_t i = 0; i < right_projection_map.size(); i++) {
			lstate.build_chunk.data[i].Reference(input.data[right_projection_map[i]]);
		}
#ifdef LINEAGE
		sink.hash_table->Build(context, lstate.join_keys, lstate.build_chunk);
#else
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
#endif
	} else if (!build_types.empty()) {
		// there is not a projected map: place the entire right chunk in the HT
#ifdef LINEAGE
		sink.hash_table->Build(context, lstate.join_keys, input);
#else
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
#endif
	} else {
		// there are only keys: place an empty chunk in the payload
		lstate.build_chunk.SetCardinality(input.size());
#ifdef LINEAGE
		sink.hash_table->Build(context, lstate.join_keys, lstate.build_chunk);
#else
		sink.hash_table->Build(lstate.join_keys, lstate.build_chunk);
#endif
	}
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
bool PhysicalHashJoin::Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	auto &sink = (HashJoinGlobalState &)*state;
	sink.hash_table->Finalize();

	PhysicalSink::Finalize(pipeline, context, move(state));
	return true;
}

//===--------------------------------------------------------------------===//
// GetChunkInternal
//===--------------------------------------------------------------------===//
class PhysicalHashJoinState : public PhysicalOperatorState {
public:
	PhysicalHashJoinState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right,
	                      vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(op, left) {
	}

	DataChunk cached_chunk;
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};

bool CanCacheType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT: {
		auto &entries = StructType::GetChildTypes(type);
		for (auto &entry : entries) {
			if (!CanCacheType(entry.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashJoin::GetOperatorState() {
	auto state = make_unique<PhysicalHashJoinState>(*this, children[0].get(), children[1].get(), conditions);
	state->cached_chunk.Initialize(types);
	state->join_keys.Initialize(condition_types);
	for (auto &cond : conditions) {
		state->probe_executor.AddExpression(*cond.left);
	}
	return move(state);
}

void PhysicalHashJoin::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                        PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_p);
	auto &sink = (HashJoinGlobalState &)*sink_state;
	bool join_is_inner_right_semi =
	    (sink.hash_table->join_type == JoinType::INNER || sink.hash_table->join_type == JoinType::RIGHT ||
	     sink.hash_table->join_type == JoinType::SEMI);

	if (sink.hash_table->Count() == 0 && join_is_inner_right_semi) {
		// empty hash table with INNER, RIGHT or SEMI join means empty result set
		return;
	}

	do {
		ProbeHashTable(context, chunk, state);
#ifdef LINEAGE
		if (state->scan_structure && state->scan_structure->lineage_probe_data) {
			lineage_op->Capture(state->scan_structure->lineage_probe_data, LINEAGE_PROBE);
		}
#endif
		if (chunk.size() == 0) {
#if STANDARD_VECTOR_SIZE >= 128
			if (state->cached_chunk.size() > 0) {
				// finished probing but cached data remains, return cached chunk
				chunk.Move(state->cached_chunk);
				state->cached_chunk.Initialize(types);
			} else
#endif
			    if (IsRightOuterJoin(join_type)) {
				// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
#ifdef LINEAGE
				context.SetCurrentLineageOp(lineage_op);
				sink.hash_table->ScanFullOuter(context, chunk, sink.ht_scan_state);
#else
				sink.hash_table->ScanFullOuter(chunk, sink.ht_scan_state);
#endif
			}
#ifdef LINEAGE
			lineage_op->MarkChunkReturned();
#endif
			return;
		} else {
#if STANDARD_VECTOR_SIZE >= 128
			if (can_cache && chunk.size() < 64) {
				// small chunk: add it to chunk cache and continue
				state->cached_chunk.Append(chunk);
				if (state->cached_chunk.size() >= (STANDARD_VECTOR_SIZE - 64)) {
					// chunk cache full: return it
					chunk.Move(state->cached_chunk);
					state->cached_chunk.Initialize(types);
#ifdef LINEAGE
					lineage_op->MarkChunkReturned();
#endif
					return;
				} else {
					// chunk cache not full: probe again
					chunk.Reset();
				}
			} else {
#ifdef LINEAGE
				lineage_op->MarkChunkReturned();
#endif
				return;
			}
#else
#ifdef LINEAGE
			lineage_op->MarkChunkReturned();
#endif
			return;
#endif
		}
	} while (true);
}

void PhysicalHashJoin::ProbeHashTable(ExecutionContext &context, DataChunk &chunk,
                                      PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalHashJoinState *>(state_p);
	auto &sink = (HashJoinGlobalState &)*sink_state;

	if (state->child_chunk.size() > 0 && state->scan_structure) {
		// still have elements remaining from the previous probe (i.e. we got
		// >1024 elements in the previous probe)
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
		if (chunk.size() > 0) {
			return;
		}
		state->scan_structure = nullptr;
	}

	// probe the HT
	do {
		// fetch the chunk from the left side
		children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
		if (state->child_chunk.size() == 0) {
			return;
		}
		if (sink.hash_table->Count() == 0) {
			ConstructEmptyJoinResult(sink.hash_table->join_type, sink.hash_table->has_null, state->child_chunk, chunk);
			return;
		}
		// resolve the join keys for the left chunk
		state->probe_executor.Execute(state->child_chunk, state->join_keys);

		// perform the actual probe
		state->scan_structure = sink.hash_table->Probe(state->join_keys);
		state->scan_structure->Next(state->join_keys, state->child_chunk, chunk);
	} while (chunk.size() == 0);
}
void PhysicalHashJoin::FinalizeOperatorState(PhysicalOperatorState &state, ExecutionContext &context) {
	auto &state_p = reinterpret_cast<PhysicalHashJoinState &>(state);
	context.thread.profiler.Flush(this, &state_p.probe_executor, "probe_executor", 0);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}
void PhysicalHashJoin::Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	auto &state = (HashJoinLocalState &)lstate;
	context.thread.profiler.Flush(this, &state.build_executor, "build_executor", 1);
	context.client.profiler->Flush(context.thread.profiler);
}

} // namespace duckdb
