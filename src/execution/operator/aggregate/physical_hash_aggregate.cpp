#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalHashAggregate(context, move(types), move(expressions), {}, estimated_cardinality, type) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality,
                                             PhysicalOperatorType type)
    : PhysicalSink(type, move(types), estimated_cardinality), groups(move(groups_p)), all_combinable(true),
      any_distinct(false) {
	// get a list of all aggregates to be computed
	// fake a single group with a constant value for aggregation without groups
	if (this->groups.empty()) {
		group_types.push_back(LogicalType::TINYINT);
		is_implicit_aggr = true;
	} else {
		is_implicit_aggr = false;
	}
	for (auto &expr : groups) {
		group_types.push_back(expr->return_type);
	}
	vector<LogicalType> payload_types_filters;
	for (auto &expr : expressions) {
		D_ASSERT(expr->expression_class == ExpressionClass::BOUND_AGGREGATE);
		D_ASSERT(expr->IsAggregate());
		auto &aggr = (BoundAggregateExpression &)*expr;
		bindings.push_back(&aggr);

		if (aggr.distinct) {
			any_distinct = true;
		}

		aggregate_return_types.push_back(aggr.return_type);
		for (auto &child : aggr.children) {
			payload_types.push_back(child->return_type);
		}
		if (aggr.filter) {
			payload_types_filters.push_back(aggr.filter->return_type);
		}
		if (!aggr.function.combine) {
			all_combinable = false;
		}
		aggregates.push_back(move(expr));
	}

	for (const auto &pay_filters : payload_types_filters) {
		payload_types.push_back(pay_filters);
	}

	// 10000 seems like a good compromise here
	radix_limit = 10000;

	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalOperatorState {
public:
	HashAggregateGlobalState(PhysicalHashAggregate &op_p, ClientContext &context)
	    : op(op_p), is_empty(true), total_groups(0),
	      partition_info((idx_t)TaskScheduler::GetScheduler(context).NumberOfThreads()) {
	}

	PhysicalHashAggregate &op;
	vector<unique_ptr<PartitionableHashTable>> intermediate_hts;
	vector<unique_ptr<GroupedAggregateHashTable>> finalized_hts;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
	//! The lock for updating the global aggregate state
	mutex lock;
	//! a counter to determine if we should switch over to p
	atomic<idx_t> total_groups;

	RadixPartitionInfo partition_info;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	explicit HashAggregateLocalState(PhysicalHashAggregate &op_p) : op(op_p), is_empty(true) {
		group_chunk.InitializeEmpty(op.group_types);
		if (!op.payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(op.payload_types);
		}

		// if there are no groups we create a fake group so everything has the same group
		if (op.groups.empty()) {
			group_chunk.data[0].Reference(Value::TINYINT(42));
		}
	}

	PhysicalHashAggregate &op;

	DataChunk group_chunk;
	DataChunk aggregate_input_chunk;

	//! The aggregate HT
	unique_ptr<PartitionableHashTable> ht;

	//! Whether or not any tuples were added to the HT
	bool is_empty;
};

unique_ptr<GlobalOperatorState> PhysicalHashAggregate::GetGlobalState(ClientContext &context) {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) {
	return make_unique<HashAggregateLocalState>(*this);
}

void PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
                                 DataChunk &input) const {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	DataChunk &group_chunk = llstate.group_chunk;
	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	for (idx_t group_idx = 0; group_idx < groups.size(); group_idx++) {
		auto &group = groups[group_idx];
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref_expr = (BoundReferenceExpression &)*group;
		group_chunk.data[group_idx].Reference(input.data[bound_ref_expr.index]);
	}
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	group_chunk.SetCardinality(input.size());
	aggregate_input_chunk.SetCardinality(input.size());

	group_chunk.Verify();
	aggregate_input_chunk.Verify();
	D_ASSERT(aggregate_input_chunk.ColumnCount() == 0 || group_chunk.size() == aggregate_input_chunk.size());

	// if we have non-combinable aggregates (e.g. string_agg) or any distinct aggregates we cannot keep parallel hash
	// tables
	if (ForceSingleHT(state)) {
		lock_guard<mutex> glock(gstate.lock);
		gstate.is_empty = gstate.is_empty && group_chunk.size() == 0;
		if (gstate.finalized_hts.empty()) {
			gstate.finalized_hts.push_back(
			    make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context.client), group_types,
			                                           payload_types, bindings, HtEntryType::HT_WIDTH_64));
		}
		D_ASSERT(gstate.finalized_hts.size() == 1);
		gstate.total_groups += gstate.finalized_hts[0]->AddChunk(group_chunk, aggregate_input_chunk);
#ifdef LINEAGE
		// TODO: don't use gstate
		lineage_op.at(context.task.thread_id)->Capture(move(gstate.finalized_hts[0]->lineage_data), LINEAGE_SINK);
#endif
		return;
	}

  // TODO: handle parallel execution

	D_ASSERT(all_combinable);
	D_ASSERT(!any_distinct);

	if (group_chunk.size() > 0) {
		llstate.is_empty = false;
	}

	if (!llstate.ht) {
		llstate.ht = make_unique<PartitionableHashTable>(BufferManager::GetBufferManager(context.client),
		                                                 gstate.partition_info, group_types, payload_types, bindings);
	}

	gstate.total_groups +=
	    llstate.ht->AddChunk(group_chunk, aggregate_input_chunk,
	                         gstate.total_groups > radix_limit && gstate.partition_info.n_partitions > 1);
#ifdef LINEAGE
	if (!llstate.ht->IsPartitioned()) {
		lineage_op.at(context.task.thread_id)->Capture(move(llstate.ht->unpartitioned_hts.back()->lineage_data), LINEAGE_SINK);
	} else {
		// handle radix_partitioned_hts case
		// persist: sel_vectors[partition]  radix_partitioned_hts[partition]->lineage_data
	}

#endif
}

class PhysicalHashAggregateState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateState(PhysicalOperator &op, vector<LogicalType> &group_types,
	                           vector<LogicalType> &aggregate_types, PhysicalOperator *child)
	    : PhysicalOperatorState(op, child), ht_index(0), ht_scan_position(0) {
		auto scan_chunk_types = group_types;
		for (auto &aggr_type : aggregate_types) {
			scan_chunk_types.push_back(aggr_type);
		}
		scan_chunk.Initialize(scan_chunk_types);
	}

	//! Materialized GROUP BY expressions & aggregates
	DataChunk scan_chunk;

	//! The current position to scan the HT for output tuples
	idx_t ht_index;
	idx_t ht_scan_position;
};

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate) {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	// this actually does not do a lot but just pushes the local HTs into the global state so we can later combine them
	// in parallel

	if (ForceSingleHT(state)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		return;
	}

	if (!llstate.ht) {
		return; // no data
	}

	if (!llstate.ht->IsPartitioned() && gstate.partition_info.n_partitions > 1 && gstate.total_groups > radix_limit) {
		llstate.ht->Partition();
	}

	lock_guard<mutex> glock(gstate.lock);
	D_ASSERT(all_combinable);
	D_ASSERT(!any_distinct);

	if (!llstate.is_empty) {
		gstate.is_empty = false;
	}

	// we will never add new values to these HTs so we can drop the first part of the HT
	llstate.ht->Finalize();
#ifdef LINEAGE
	llstate.ht->thread_id = context.task.thread_id;
#endif
	// at this point we just collect them the PhysicalHashAggregateFinalizeTask (below) will merge them in parallel
	gstate.intermediate_hts.push_back(move(llstate.ht));
}

// this task is run in multiple threads and combines the radix-partitioned hash tables into a single onen and then
// folds them into the global ht finally.
class PhysicalHashAggregateFinalizeTask : public Task {
public:
	PhysicalHashAggregateFinalizeTask(Pipeline &parent_p, HashAggregateGlobalState &state_p, idx_t radix_p)
	    : parent(parent_p), state(state_p), radix(radix_p) {
	}
	static void FinalizeHT(HashAggregateGlobalState &gstate, idx_t radix) {
		D_ASSERT(gstate.finalized_hts[radix]);
		for (auto &pht : gstate.intermediate_hts) {
			for (auto &ht : pht->GetPartition(radix)) {
				gstate.finalized_hts[radix]->Combine(*ht);
#ifdef LINEAGE
				gstate.finalized_hts[radix]->combine_lineage_data.clear();
#endif
				ht.reset();
			}
		}
		gstate.finalized_hts[radix]->Finalize();
	}

	void Execute() override {
		FinalizeHT(state, radix);
		auto total_tasks = parent.total_tasks.load();
		auto finished_tasks = ++parent.finished_tasks;
		// finish the whole pipeline
		if (total_tasks == finished_tasks) {
			parent.Finish();
		}
	}

private:
	Pipeline &parent;
	HashAggregateGlobalState &state;
	idx_t radix;
};

bool PhysicalHashAggregate::Finalize(Pipeline &pipeline, ClientContext &context,
                                     unique_ptr<GlobalOperatorState> state) {
	return FinalizeInternal(context, move(state), false, &pipeline);
}

void PhysicalHashAggregate::FinalizeImmediate(ClientContext &context, unique_ptr<GlobalOperatorState> state) {
	FinalizeInternal(context, move(state), true, nullptr);
}

bool PhysicalHashAggregate::FinalizeInternal(ClientContext &context, unique_ptr<GlobalOperatorState> state,
                                             bool immediate, Pipeline *pipeline) {
	this->sink_state = move(state);
	auto &gstate = (HashAggregateGlobalState &)*this->sink_state;

	// special case if we have non-combinable aggregates
	// we have already aggreagted into a global shared HT that does not require any additional finalization steps
	if (ForceSingleHT(gstate)) {
		D_ASSERT(gstate.finalized_hts.size() <= 1);
		return true;
	}

	// we can have two cases now, non-partitioned for few groups and radix-partitioned for very many groups.
	// go through all of the child hts and see if we ever called partition() on any of them
	// if we did, its the latter case.
	bool any_partitioned = false;
	for (auto &pht : gstate.intermediate_hts) {
		if (pht->IsPartitioned()) {
			any_partitioned = true;
			break;
		}
	}

  // TODO: check how lineage would change if this path was taken
	if (any_partitioned) {
		// if one is partitioned, all have to be
		// this should mostly have already happened in Combine, but if not we do it here
		for (auto &pht : gstate.intermediate_hts) {
			if (!pht->IsPartitioned()) {
				pht->Partition();
			}
		}
		// schedule additional tasks to combine the partial HTs
		if (!immediate) {
			D_ASSERT(pipeline);
			pipeline->total_tasks += gstate.partition_info.n_partitions;
		}
		gstate.finalized_hts.resize(gstate.partition_info.n_partitions);
		for (idx_t r = 0; r < gstate.partition_info.n_partitions; r++) {
			gstate.finalized_hts[r] =
			    make_unique<GroupedAggregateHashTable>(BufferManager::GetBufferManager(context), group_types,
			                                           payload_types, bindings, HtEntryType::HT_WIDTH_64);
			if (immediate) {
				PhysicalHashAggregateFinalizeTask::FinalizeHT(gstate, r);
			} else {
				D_ASSERT(pipeline);
				auto new_task = make_unique<PhysicalHashAggregateFinalizeTask>(*pipeline, gstate, r);
				TaskScheduler::GetScheduler(context).ScheduleTask(pipeline->token, move(new_task));
			}
		}
		return immediate;
	} else { // in the non-partitioned case we immediately combine all the unpartitioned hts created by the threads.
		     // TODO possible optimization, if total count < limit for 32 bit ht, use that one
		     // create this ht here so finalize needs no lock on gstate

		gstate.finalized_hts.push_back(make_unique<GroupedAggregateHashTable>(
		    BufferManager::GetBufferManager(context), group_types, payload_types, bindings, HtEntryType::HT_WIDTH_64));
		for (auto &pht : gstate.intermediate_hts) {
			auto unpartitioned = pht->GetUnpartitioned();
			for (auto &unpartitioned_ht : unpartitioned) {
				D_ASSERT(unpartitioned_ht);
				gstate.finalized_hts[0]->Combine(*unpartitioned_ht);
#ifdef LINEAGE
				for (idx_t i=0; gstate.finalized_hts[0]->combine_lineage_data.size(); ++i) {
				  lineage_op.begin()->second->Capture(move(gstate.finalized_hts[0]->combine_lineage_data[i]), LINEAGE_COMBINE, pht->thread_id);
				  gstate.finalized_hts[0]->combine_lineage_data.clear();
				}
#endif
				unpartitioned_ht.reset();
			}
			unpartitioned.clear();
		}
		gstate.finalized_hts[0]->Finalize();
		return true;
	}
}

void PhysicalHashAggregate::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                             PhysicalOperatorState *state_p) const {
	auto &gstate = (HashAggregateGlobalState &)*sink_state;
	auto &state = (PhysicalHashAggregateState &)*state_p;

	state.scan_chunk.Reset();

#ifdef LINEAGE
	context.SetCurrentLineageOp(lineage_op.at(context.task.thread_id));
#endif

	// special case hack to sort out aggregating from empty intermediates
	// for aggregations without groups
	if (gstate.is_empty && is_implicit_aggr) {
		D_ASSERT(chunk.ColumnCount() == aggregates.size());
		// for each column in the aggregates, set to initial state
		chunk.SetCardinality(1);
		for (idx_t i = 0; i < chunk.ColumnCount(); i++) {
			D_ASSERT(aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = (BoundAggregateExpression &)*aggregates[i];
			auto aggr_state = unique_ptr<data_t[]>(new data_t[aggr.function.state_size()]);
			aggr.function.initialize(aggr_state.get());

			Vector state_vector(Value::POINTER((uintptr_t)aggr_state.get()));
			aggr.function.finalize(state_vector, aggr.bind_info.get(), chunk.data[i], 1, 0);
			if (aggr.function.destructor) {
				aggr.function.destructor(state_vector, 1);
			}
		}
		state.finished = true;
		return;
	}
	if (gstate.is_empty && !state.finished) {
		state.finished = true;
		return;
	}
	idx_t elements_found = 0;

	while (true) {
		if (state.ht_index == gstate.finalized_hts.size()) {
			state.finished = true;
			return;
		}
#ifdef LINEAGE
		elements_found = gstate.finalized_hts[state.ht_index]->Scan(context, state.ht_scan_position, state.scan_chunk);
#else
		elements_found = gstate.finalized_hts[state.ht_index]->Scan(state.ht_scan_position, state.scan_chunk);
#endif

		if (elements_found > 0) {
			break;
		}
		gstate.finalized_hts[state.ht_index].reset();
		state.ht_index++;
		state.ht_scan_position = 0;
	}

	// compute the final projection list
	idx_t chunk_index = 0;
	chunk.SetCardinality(elements_found);
	if (group_types.size() + aggregates.size() == chunk.ColumnCount()) {
		for (idx_t col_idx = 0; col_idx < group_types.size(); col_idx++) {
			chunk.data[chunk_index++].Reference(state.scan_chunk.data[col_idx]);
		}
	} else {
		D_ASSERT(aggregates.size() == chunk.ColumnCount());
	}

	for (idx_t col_idx = 0; col_idx < aggregates.size(); col_idx++) {
		chunk.data[chunk_index++].Reference(state.scan_chunk.data[group_types.size() + col_idx]);
	}
}

unique_ptr<PhysicalOperatorState> PhysicalHashAggregate::GetOperatorState() {
	return make_unique<PhysicalHashAggregateState>(*this, group_types, aggregate_return_types,
	                                               children.empty() ? nullptr : children[0].get());
}

bool PhysicalHashAggregate::ForceSingleHT(GlobalOperatorState &state) const {
	auto &gstate = (HashAggregateGlobalState &)state;

	return !all_combinable || any_distinct || gstate.partition_info.n_partitions < 2;
}

string PhysicalHashAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
