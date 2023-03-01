#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/lineage/operator_lineage.hpp"
#include "duckdb/execution/lineage/lineage_query.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/index/lineage_index/lineage_index.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/scan/physical_empty_result.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"

#include <utility>

#define PROBE_SIZE 10

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;

unique_ptr<PhysicalOperator> GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	vector<unique_ptr<PhysicalOperator>> *pipelines
);

unique_ptr<PhysicalOperator> CombineByMode(
	ClientContext &context,
	const string& mode,
	bool should_count,
	unique_ptr<PhysicalOperator> first_plan,
	vector<unique_ptr<PhysicalOperator>> other_plans
);

// Post Processing to prepare for querying

void LineageManager::PostProcess(PhysicalOperator *op, bool should_index) {
	// massage the data to make it easier to query
//	bool always_post_process =
//		op->type == PhysicalOperatorType::HASH_GROUP_BY || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY;
//	bool never_post_process =
//		op->type == PhysicalOperatorType::ORDER_BY; // 1 large chunk, so index is useless
//	if ((always_post_process) && !never_post_process) {
//		vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
//		for (idx_t i = 0; i < table_column_types.size(); i++) {
//			bool skip_this_sel_vec =
//				(op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_COMBINE)
//				|| (op->type == PhysicalOperatorType::HASH_JOIN && i == LINEAGE_BUILD)
//				|| (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::MARK)
//				|| (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_COMBINE)
//				|| (op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_SOURCE)
//				|| (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_SOURCE);
//			if (skip_this_sel_vec) {
//				continue;
//			}
//			// for hash join, build hash table on the build side that map the address to id
//			// for group by, build hash table on the unique groups
//			for (auto const& lineage_op : op->lineage_op) {
//				idx_t chunk_count = 0;
//				LineageProcessStruct lps = lineage_op.second->PostProcess(chunk_count, 0, lineage_op.first);
//				while (lps.still_processing) {
//					lps = lineage_op.second->PostProcess(++chunk_count,  lps.count_so_far, lps.data_idx, lps.finished_idx);
//				}
//				lineage_op.second->FinishedProcessing(lps.data_idx, lps.finished_idx);
//			}
//		}
//	}
//
//	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
//		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->children[0].get(), true);
//		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), true);
//		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), true);
//		return;
//	}
//	for (idx_t i = 0; i < op->children.size(); i++) {
//		bool child_should_index =
//			op->type == PhysicalOperatorType::HASH_GROUP_BY
//			|| op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
//			|| (op->type == PhysicalOperatorType::HASH_JOIN && i == 1) // Only build side child needs an index
//			|| (op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN && i == 1) // Right side needs index
//			|| (op->type == PhysicalOperatorType::CROSS_PRODUCT && i == 1) // Right side needs index
//			|| (op->type == PhysicalOperatorType::NESTED_LOOP_JOIN && i == 1) // Right side needs index
//			|| (op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN && i == 1) // Right side needs index
//			|| op->type == PhysicalOperatorType::ORDER_BY
//			|| (op->type == PhysicalOperatorType::PROJECTION && should_index); // Pass through should_index on projection
//		PostProcess(op->children[i].get(), child_should_index);
//	}
}

LineageProcessStruct OperatorLineage::PostProcess(idx_t chunk_count, idx_t count_so_far, idx_t data_idx, idx_t finished_idx) {
//	std::cout << "Postprocess " << PhysicalOperatorToString(this->type) << this->opid << std::endl;
	if (data[finished_idx].size() > data_idx) {
		switch (this->type) {
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::INDEX_JOIN:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			// Array index
			if (chunk_count == 0) {
				// Reserve index array
				LineageDataWithOffset last_data = data[LINEAGE_UNARY][data[LINEAGE_UNARY].size() - 1];
				index.reserve(last_data.child_offset + last_data.data->Size());
			}
			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();
			index.reserve(index.size() + res_count);
			for (idx_t i = 0; i < res_count; i++) {
				index.push_back(chunk_count);
			}
			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			// Hash Join - other joins too?
			if (finished_idx == LINEAGE_BUILD) {
				// Shouldn't hit this code path
				D_ASSERT(false);
			} else {
				// Array index
				if (chunk_count == 0) {
					// Reserve index array
					LineageDataWithOffset last_data = data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1];
					index.reserve(last_data.child_offset + last_data.data->Size());
				}
				LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
				idx_t res_count = this_data.data->Count();
				for (idx_t i = 0; i < res_count; i++) {
					index.push_back(chunk_count);
				}
			}
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			// Hash Aggregate / Perfect Hash Aggregate
			// schema for both: [INTEGER in_index, INTEGER out_index]
			if (finished_idx == LINEAGE_SINK) {
				// build hash table
//				LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
//				idx_t res_count = this_data.data->Count();
//				if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
//					auto payload = (sel_t*)this_data.data->Process(0);
//					for (idx_t i=0; i < res_count; ++i) {
//						idx_t bucket = payload[i];
//						if (hash_map_agg[bucket] == nullptr) {
//							hash_map_agg[bucket] = make_shared<vector<SourceAndMaybeData>>();
//						}
//						auto child = this_data.data->GetChild();
//						auto val = i + count_so_far;
//						if (child != nullptr) {
//							// We capture global value, so we convert to child local value here
//							val -= child->this_offset;
//						}
//						hash_map_agg[bucket]->push_back({val, child});
//					}
//				} else {
//					auto payload = (uint64_t*)this_data.data->Process(0);
//					for (idx_t i=0; i < res_count; ++i) {
//						idx_t bucket = payload[i];
//						if (hash_map_agg[bucket] == nullptr) {
//							hash_map_agg[bucket] = make_shared<vector<SourceAndMaybeData>>();
//						}
//						auto child = this_data.data->GetChild();
//						auto val = i + count_so_far;
//						if (child != nullptr) {
//							// We capture global value, so we convert to child local value here
//							val -= child->this_offset;
//						}
//						hash_map_agg[bucket]->push_back({val, child});
//					}
//				}
//				count_so_far += res_count;
			} else if (finished_idx == LINEAGE_COMBINE) {
			} else {
				// Array index
				if (chunk_count == 0) {
					// Reserve index array
					LineageDataWithOffset last_data = data[LINEAGE_SOURCE][data[LINEAGE_SOURCE].size() - 1];
					index.reserve(last_data.child_offset + last_data.data->Size());
				}
				LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
				idx_t res_count = this_data.data->Count();
				for (idx_t i = 0; i < res_count; i++) {
					index.push_back(chunk_count);
				}
			}
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			// Array index
			if (chunk_count == 0) {
				// Reserve index array
				LineageDataWithOffset last_data = data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1];
				index.reserve(last_data.child_offset + last_data.data->Size());
			}
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();
			for (idx_t i = 0; i < res_count; i++) {
				index.push_back(chunk_count);
			}
			break;
		}
		case PhysicalOperatorType::ORDER_BY: {
			throw std::logic_error("Shouldn't post-process ORDER_BY");
		}
		default:
			// We must capture lineage for everything getting post-processed
			D_ASSERT(false);
		}
	}
	data_idx++;
	return LineageProcessStruct{ count_so_far, 0, data_idx, finished_idx, data[finished_idx].size() > data_idx};
}

template <typename T>
void Reverse(vector<unique_ptr<T>> *vec) {
	for (idx_t i = 0; i < vec->size() / 2; i++) {
		unique_ptr<T> tmp = move(vec->at(i));
		vec->at(i) = move(vec->at(vec->size() - i - 1));
		vec->at(vec->size() - i - 1) = move(tmp);
	}
}

unique_ptr<QueryResult> LineageQuery::Run(
    PhysicalOperator *op,
    ClientContext &context,
    const string& mode,
    int lineage_id,
    bool should_count
) {
	vector<unique_ptr<PhysicalOperator>> other_plans;
	unique_ptr<PhysicalOperator> first_plan = GenerateCustomPlan(op, context, lineage_id, nullptr, &other_plans);
	// We construct other_plans in reverse execution order, swap here
	Reverse(&other_plans);

	unique_ptr<PhysicalOperator> final_plan = CombineByMode(context, mode, should_count, move(first_plan), move(other_plans));
	return context.RunPlan(final_plan.get());
}

// Lineage Query Plan Constructing

unique_ptr<PhysicalIndexJoin> PreparePhysicalIndexJoin(PhysicalOperator *op, unique_ptr<PhysicalOperator> left, ClientContext &cxt, ChunkCollection *chunk_collection) {
	auto logical_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
	logical_join->types = {LogicalType::UBIGINT};
	vector<JoinCondition> conds;
	unique_ptr<Expression> left_expression = make_unique<BoundReferenceExpression>("", LogicalType::UBIGINT, 0);
	unique_ptr<Expression> right_expression = make_unique<BoundReferenceExpression>("", LogicalType::UBIGINT, 0);
	JoinCondition cond = JoinCondition();
	cond.left = move(left_expression);
	cond.right = move(right_expression);
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	cond.null_values_are_equal = false;
	conds.push_back(move(cond));
	vector<LogicalType> empty_types = {LogicalType::UBIGINT};
	const vector<column_t> cids = {0L};
	unique_ptr<ColumnBinding> col_bind = make_unique<ColumnBinding>(0,0);
	unique_ptr<BoundColumnRefExpression> exp = make_unique<BoundColumnRefExpression>(LogicalType::UBIGINT, *col_bind.get());
	vector<unique_ptr<Expression>> exps;
	exps.push_back(move(exp));
	unique_ptr<Index> lineage_index = make_unique<LineageIndex>(cids, exps, op->lineage_op.at(-1));
	lineage_index->unbound_expressions.push_back(move(exp));
	lineage_index->column_ids.push_back(0);
	vector<idx_t> left_projection_map = {};
	vector<idx_t> right_projection_map = {0};
	vector<column_t> column_ids = {0};
	unique_ptr<PhysicalOperator> right = make_unique<PhysicalEmptyResult>(empty_types, 1);
	return make_unique<PhysicalIndexJoin>(
		*logical_join.get(),
		move(left),
		move(right),
		move(conds),
		JoinType::INNER,
		left_projection_map,
		right_projection_map,
		column_ids,
		lineage_index.get(),
		false,
		1, // TODO improve this?
		true,
		op->lineage_op.at(-1),
		chunk_collection
	);
}

unique_ptr<PhysicalOperator> GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	vector<unique_ptr<PhysicalOperator>> *pipelines
) {
	if (!op) {
		return left;
	}
	if (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type == JoinType::MARK) {
		// Skip Mark Joins
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), pipelines);
	}
	if (op->type == PhysicalOperatorType::PROJECTION) {
		// Skip Projections
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), pipelines);
	}
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		// Skip DELIM_SCANs since they never affect the lineage
		return left;
	}
	if (op->children.empty()) {
		return PreparePhysicalIndexJoin(op, move(left), cxt, nullptr);
	}
//	if (simple_agg_flag && (
//		op->type == PhysicalOperatorType::HASH_GROUP_BY ||
//		op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
//		op->type == PhysicalOperatorType::SIMPLE_AGGREGATE ||
//		op->type == PhysicalOperatorType::ORDER_BY ||
//		op->type == PhysicalOperatorType::PROJECTION
//	)) {
//		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), true, pipelines);
//	}
	vector<LogicalType> types = {LogicalType::UBIGINT};
	PhysicalOperatorType op_type = PhysicalOperatorType::CHUNK_SCAN;
	idx_t estimated_cardinality = 1;

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		if (!op->delim_handled) {
			// TODO handle multithreading here?
			idx_t thread_id = -1;

			// set this child to join's child to appropriately line up chunk scan lineage
			dynamic_cast<PhysicalDelimJoin *>(op)->join->children[0] = move(op->children[0]);

			// distinct input is delim join input
			// distinct should be the input to delim scan
			op->lineage_op[thread_id]->children[2]->children.push_back(op->lineage_op[thread_id]->children[0]);

			// chunk scan input is delim join input
			op->lineage_op[thread_id]->children[1]->children[1] = op->lineage_op[thread_id]->children[0];

			// we only want to do the child reordering once
			op->delim_handled = true;
		}
		return GenerateCustomPlan(dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), cxt, lineage_id, move(left), pipelines);
	}
	if (left == nullptr) {
		unique_ptr<PhysicalChunkScan> chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
		DataChunk lineage_chunk;
		lineage_chunk.Initialize(types);
		lineage_chunk.SetCardinality(1);
		Vector lineage(Value::UBIGINT(lineage_id));
		lineage_chunk.data[0].Reference(lineage);
		chunk_scan->collection = new ChunkCollection();
		chunk_scan->collection->Append(lineage_chunk);
		if (
			op->children.size() == 2 &&
			(op->type == PhysicalOperatorType::INDEX_JOIN || op->type == PhysicalOperatorType::CROSS_PRODUCT || dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI)
			) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, build_chunk_scan->collection), pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), pipelines));
			}

			return custom_plan;
		}
		return GenerateCustomPlan(
			op->children[0].get(),
			cxt,
			lineage_id,
			PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, nullptr),
			pipelines
		);
	} else {
		if (op->children.size() == 2 && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(left), cxt,  build_chunk_scan->collection), pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), pipelines));
			}

			// probe side of hash join
			return custom_plan;
		}
		return GenerateCustomPlan(
			op->children[0].get(),
			cxt,
			lineage_id,
			PreparePhysicalIndexJoin(op, move(left), cxt, nullptr),
			pipelines
		);
	}
}

unique_ptr<PhysicalOperator> CombineByMode(
	ClientContext &context,
	const string& mode,
	bool should_count,
	unique_ptr<PhysicalOperator> first_plan,
	vector<unique_ptr<PhysicalOperator>> other_plans
) {
	unique_ptr<PhysicalOperator> final_plan = move(first_plan);
	if (mode == "LIN") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		// Count of Lineage - union then aggregate
		for (idx_t i = 0; i < other_plans.size(); i++) {
			final_plan = make_unique<PhysicalUnion>(
				types,
				move(final_plan),
				move(other_plans[i]),
				1 // TODO improve this?
			);
		}
	} else if (mode == "PERM") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		for (idx_t i = 0; i < other_plans.size(); i++) {
			types.push_back(LogicalType::UBIGINT);
			final_plan = make_unique<PhysicalCrossProduct>(
				types,
				move(other_plans[i]),
				move(final_plan),
				1 // TODO improve this?
			);
		}
	} else {
		// Invalid mode
		throw std::logic_error("Invalid backward query mode - should one of [LIN, PERM, PROV]");
	}
	if (should_count) {
		// Add final aggregation at end
		vector<LogicalType> types = {LogicalType::UBIGINT};
		vector<unique_ptr<Expression>> agg_exprs;

		agg_exprs.push_back(AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}));
		unique_ptr<PhysicalOperator> agg = make_unique<PhysicalSimpleAggregate>(types, move(agg_exprs), true, 1);

		agg->children.push_back(move(final_plan));
		final_plan = move(agg);
	}
	return final_plan;
}

// Lineage Querying

vector<shared_ptr<LineageDataWithOffset>> LookupChunksFromGlobalIndex(
    DataChunk &chunk,
    const vector<LineageDataWithOffset>& data,
    const vector<idx_t>& index
) {
	vector<shared_ptr<LineageDataWithOffset>> res;
	res.reserve(chunk.size());
	// Binary Search index
	for (idx_t i = 0; i < chunk.size(); i++) {
		idx_t val = chunk.GetValue(0, i).GetValue<uint64_t>();
		// we need a way to locate the exact data we should access
		// from the source index
		auto lower = lower_bound(index.begin(), index.end(), val);
		if (lower == index.end() || (lower == index.end() - 1 && *lower == val)) {
			throw std::logic_error("Out of bounds lineage requested");
		}
		auto chunk_id = lower - index.begin();
		if (*lower == val) {
			chunk_id += 1;
		}
		auto this_data = data[chunk_id];
		if (chunk_id > 0) {
			val -= index[chunk_id-1];
		}
		chunk.SetValue(0, i, Value::UBIGINT(val));
		res.push_back(make_unique<LineageDataWithOffset>(this_data));
	}
	return res;
}

void LookupAggChunksFromGlobalIndex(
    DataChunk &chunk,
    const vector<LineageDataWithOffset>& data,
    const vector<idx_t>& index
) {
	// Binary Search index
	for (idx_t i = 0; i < chunk.lineage_agg_data->size(); i++) {
		auto this_agg_data = chunk.lineage_agg_data->at(0);
		for (idx_t j = 0; j < this_agg_data->size(); j++) {
			idx_t val = this_agg_data->at(j).source;
			// we need a way to locate the exact data we should access
			// from the source index
			auto lower = lower_bound(index.begin(), index.end(), val);
			if (lower == index.end() || (lower == index.end() - 1 && *lower == val)) {
				throw std::logic_error("Out of bounds lineage requested");
			}
			auto chunk_id = lower - index.begin();
			if (*lower == val) {
				chunk_id += 1;
			}
			auto this_data = data[chunk_id];
			if (chunk_id > 0) {
				val -= index[chunk_id-1];
			}
			this_agg_data->at(j).source = val;
			this_agg_data->at(j).data = make_shared<LineageDataWithOffset>(this_data);
		}
	}
}

shared_ptr<vector<LineageDataWithOffset>> OperatorLineage::RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child) {
	vector<LineageDataWithOffset> child_lineage_data_vector;
	switch (child->type) {
	case PhysicalOperatorType::PROJECTION: {
		return RecurseForSimpleAgg(child->children[0]);
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Normally we recurse, but we don't because of ablation study
		return make_shared<vector<LineageDataWithOffset>>(child->data[LINEAGE_SINK]); // LINEAGE_SINK == LINEAGE_UNARY
	}
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::INDEX_JOIN: {
		return make_shared<vector<LineageDataWithOffset>>(child->data[LINEAGE_UNARY]);
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		return make_shared<vector<LineageDataWithOffset>>(child->data[LINEAGE_PROBE]);
	}
	default:
		D_ASSERT(false);
	}

	throw std::logic_error("We must capture lineage for everything that RecurseForSimpleAgg is called on");
}

void OperatorLineage::AccessIndex(LineageIndexStruct key) {
//	std::cout << "AccessIndex " << PhysicalOperatorToString(this->type) << this->opid << std::endl;
//	for (idx_t i = 0; i < key.chunk.size(); i++) {
//		std::cout << key.chunk.GetValue(0,i) << std::endl;
//	}
	switch (this->type) {
	case (PhysicalOperatorType::DELIM_JOIN): {
		// These should have been removed from the query plan
		D_ASSERT(false);
		break;
	}
	case PhysicalOperatorType::DELIM_SCAN: {
		// These should have been removed from the query plan
		D_ASSERT(false);
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		if (data[LINEAGE_UNARY].empty() && key.child_ptrs[0] == nullptr) {
			// Nothing to do! Lineage correct as-is
		} else {
			idx_t out_idx = 0;
			if (!key.chunk.lineage_agg_data->empty()) {
				if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
					LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
				}
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
					auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
					while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
						auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
						idx_t init_val = this_data.data->data->Backward(this_data.source);
						idx_t val = init_val + this_data.data->child_offset;
						key.chunk.SetValue(0, out_idx++, Value::UBIGINT(val));
						key.chunk.inner_agg_idx++;
					}
					if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
						break;
					}
					key.chunk.inner_agg_idx = 0;
					key.chunk.outer_agg_idx++;
				}
				key.chunk.SetCardinality(out_idx);
			} else if (!key.chunk.lineage_simple_agg_data->empty()) {
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
					LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
					while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
						idx_t val = this_data.data->Backward(key.chunk.inner_simple_agg_idx) + this_data.child_offset;
						key.chunk.SetValue(0, out_idx++, Value::UBIGINT(val));
						key.chunk.inner_simple_agg_idx++;
					}
					if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
						break;
					}
					key.chunk.inner_simple_agg_idx = 0;
					key.chunk.outer_simple_agg_idx++;
				}
				key.chunk.SetCardinality(out_idx);
			} else {
				if (key.child_ptrs[0] == nullptr) {
					key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
				}
				for (idx_t i = 0; i < key.chunk.size(); i++) {
					key.chunk.SetValue(
					    0,
					    i,
					    Value::UBIGINT(key.child_ptrs[i]->data->Backward(key.chunk.GetValue(0, i).GetValue<uint64_t>()) + key.child_ptrs[i]->child_offset)
					);
				}
			}
		}
		break;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT: {
		idx_t out_idx = 0;
		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					idx_t new_val = this_data.data->data->Backward(this_data.source);
					key.chunk.SetValue(0, out_idx, Value::UBIGINT(new_val));
					key.child_ptrs[out_idx++] = this_data.data->data->GetChild();
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					idx_t new_val = this_data.data->Backward(key.chunk.inner_simple_agg_idx);
					key.chunk.SetValue(0, out_idx, Value::UBIGINT(new_val));
					key.child_ptrs[out_idx++] = this_data.data->GetChild();
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
			}
			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();
				idx_t new_val = key.child_ptrs[i]->data->Backward(source);
				key.chunk.SetValue(0, i, Value::UBIGINT(new_val));
				key.child_ptrs[i] = key.child_ptrs[i]->data->GetChild();
			}
		}
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		idx_t out_idx = 0;
		idx_t right_idx = 0;
		idx_t left_idx = 0;

		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					int data_index = dynamic_cast<LineageNested &>(*this_data.data->data).LocateChunkIndex(this_data.source);
					LineageBinary& binary_data = dynamic_cast<LineageBinary &>(*(dynamic_cast<LineageNested &>(*this_data.data->data).GetChunkAt(data_index))->data);
					idx_t adjust_offset = 0;
					if (data_index > 0) {
						// adjust the source
						adjust_offset = dynamic_cast<LineageNested &>(*this_data.data->data).GetAccCount(data_index - 1);
					}
					if (binary_data.right != nullptr) {
						key.chunk.SetValue(0,right_idx,Value::UBIGINT(binary_data.right->Backward(this_data.source - adjust_offset)));
						key.child_ptrs[right_idx] = binary_data.GetChild();
						right_idx++;
					}

					if (binary_data.left != nullptr && join_type != JoinType::ANTI) { // Skip anti joins
						auto left = binary_data.left->Backward(this_data.source - adjust_offset);
						if (left == 0) {
							return;
						}
						if (offset == 0) {
							key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(0));
						} else {
							bool flag = false;
							for (idx_t it = 0; it < hm_range.size(); ++it) {
								if (left >= hm_range[it].first && left <= hm_range[it].second) {
									auto val = ((left - hm_range[it].first) / offset) + hash_chunk_count[it];
									key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(val));
									flag = true;
									break;
								}
							}
							D_ASSERT(flag);
						}
						left_idx++;
					}
					out_idx++;
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					int data_index = dynamic_cast<LineageNested &>(*this_data.data).LocateChunkIndex(key.chunk.inner_simple_agg_idx);
					LineageBinary& binary_data = dynamic_cast<LineageBinary &>(*(dynamic_cast<LineageNested &>(*this_data.data).GetChunkAt(data_index))->data);
					idx_t adjust_offset = 0;
					if (data_index > 0) {
						// adjust the source
						adjust_offset = dynamic_cast<LineageNested &>(*this_data.data).GetAccCount(data_index - 1);
					}
					if (binary_data.right != nullptr) {
						key.chunk.SetValue(0,right_idx,Value::UBIGINT(binary_data.right->Backward(key.chunk.inner_simple_agg_idx - adjust_offset)));
						key.child_ptrs[right_idx] = binary_data.GetChild();
						right_idx++;
					}

					if (binary_data.left != nullptr && join_type != JoinType::ANTI) { // Skip anti joins
						auto left = binary_data.left->Backward(key.chunk.inner_simple_agg_idx - adjust_offset);
						if (left == 0) {
							return;
						}
						if (offset == 0) {
							key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(0));
						} else {
							bool flag = false;
							for (idx_t it = 0; it < hm_range.size(); ++it) {
								if (left >= hm_range[it].first && left <= hm_range[it].second) {
									auto val = ((left - hm_range[it].first) / offset) + hash_chunk_count[it];
									key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(val));
									flag = true;
									break;
								}
							}
							D_ASSERT(flag);
						}
						left_idx++;
					}
					out_idx++;
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
		} else {
			// we need hash table from the build side
			// access the probe side, get the address from the right side
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();
				auto data_index = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).LocateChunkIndex(source);
				auto binary_data = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).GetChunkAt(data_index);
				idx_t adjust_offset = 0;
				if (data_index > 0) {
					// adjust the source
					adjust_offset = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).GetAccCount(data_index - 1);
				}
				if (dynamic_cast<LineageBinary &>(*binary_data->data).right != nullptr) {
					key.chunk.SetValue(
					    0, right_idx,
					    Value::UBIGINT(
					        dynamic_cast<LineageBinary &>(*binary_data->data).right->Backward(source - adjust_offset)));
					key.child_ptrs[right_idx++] = binary_data->data->GetChild();
				}

				if (dynamic_cast<LineageBinary &>(*binary_data->data).left != nullptr && join_type != JoinType::ANTI) { // Skip anti joins
					auto left =
					    dynamic_cast<LineageBinary &>(*binary_data->data).left->Backward(source - adjust_offset);
					if (left == 0) {
						continue;
					}
					if (offset == 0) {
						key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(0));
					} else {
						bool flag = false;
						for (idx_t it = 0; it < hm_range.size(); ++it) {
							if (left >= hm_range[it].first && left <= hm_range[it].second) {
								auto val = ((left - hm_range[it].first) / offset) + hash_chunk_count[it];
								key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(val));
								flag = true;
								break;
							}
						}
						D_ASSERT(flag);
					}
					left_idx++;
				}
			}
		}
		// Set cardinality of chunks
		key.chunk.SetCardinality(right_idx);
		key.join_chunk.SetCardinality(left_idx);

		// Clear out child pointers for values that didn't have a right match
		for (; right_idx < key.chunk.size(); right_idx++) {
			key.child_ptrs[right_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		key.chunk.next_lineage_agg_data->reserve(key.chunk.size());
		idx_t out_idx = 0;
		unordered_set<idx_t> matches;
		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					auto payload = (uint64_t *)this_data.data->data->Process(0);
					matches.insert(payload[this_data.source]);
					out_idx++;
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					auto payload = (uint64_t *)this_data.data->Process(0);
					matches.insert(payload[key.chunk.inner_simple_agg_idx]);
					out_idx++;
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				auto payload = (uint64_t *)key.child_ptrs[i]->data->Process(0);
				matches.insert(payload[key.chunk.GetValue(0, i).GetValue<uint64_t>()]);
				out_idx++;
			}
		}

		auto vec = make_shared<vector<SourceAndMaybeData>>();
		idx_t count_so_far = 0;
		for (idx_t i = 0; i < data[LINEAGE_SINK].size(); i++) {
			auto payload = (uint64_t *)data[LINEAGE_SINK][i].data->Process(0);
			idx_t res_count = data[LINEAGE_SINK][i].data->Count();
			for (idx_t j = 0; j < res_count; ++j) {
				if (matches.find(payload[j]) != matches.end()) {
					auto child = data[LINEAGE_SINK][i].data->GetChild();
					auto val = j + count_so_far;
					if (child != nullptr) {
						// We capture global value, so we convert to child local value here
						val -= child->this_offset;
					}
					vec->push_back({val, child});
				}
			}
			count_so_far += res_count;
		}

		key.chunk.next_lineage_agg_data->push_back(vec);
		key.child_ptrs = {};
		// TODO: we never set Cardinality on chunk because we're actually returning more than 1024 values - is this okay?
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		idx_t out_idx = 0;
		unordered_set<idx_t> matches;
		if (!key.chunk.lineage_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
					LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
				}
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					auto payload = (sel_t *)this_data.data->data->Process(0);
					matches.insert(payload[this_data.source]);
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					auto payload = (sel_t *)this_data.data->Process(0);
					matches.insert(payload[key.chunk.inner_simple_agg_idx]);
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				auto payload = (sel_t *)key.child_ptrs[i]->data->Process(0);
				matches.insert(payload[key.chunk.GetValue(0, i).GetValue<uint64_t>()]);
				out_idx++;
			}
		}

		auto vec = make_shared<vector<SourceAndMaybeData>>();
		idx_t count_so_far = 0;
		for (idx_t i = 0; i < data[LINEAGE_SINK].size(); i++) {
			auto payload = (uint64_t *)data[LINEAGE_SINK][i].data->Process(0);
			idx_t res_count = data[LINEAGE_SINK][i].data->Count();
			for (idx_t j = 0; j < res_count; ++j) {
				if (matches.find(payload[j]) != matches.end()) {
					auto child = data[LINEAGE_SINK][i].data->GetChild();
					auto val = j + count_so_far;
					if (child != nullptr) {
						// We capture global value, so we convert to child local value here
						val -= child->this_offset;
					}
					vec->push_back({val, child});
				}
			}
			count_so_far += res_count;
		}

		key.chunk.next_lineage_agg_data->push_back(vec);
		key.child_ptrs = {};
		// TODO: we never set Cardinality on chunk because we're actually returning more than 1024 values - is this okay?
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		// These should have been removed from the query plan
		D_ASSERT(false);
		break;
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		idx_t out_idx = 0;
		idx_t right_idx = 0;
		idx_t left_idx = 0;

		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					LineageBinary &binary_lineage = dynamic_cast<LineageBinary &>(*this_data.data->data);
					if (binary_lineage.right != nullptr) {
						auto right = binary_lineage.right->Backward(this_data.source);
						key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
					}

					if (binary_lineage.left != nullptr) {
						auto left = binary_lineage.left->Backward(this_data.source);
						key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
						key.child_ptrs[left_idx++] = binary_lineage.GetChild();
					}
					out_idx++;
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					LineageBinary &binary_lineage = dynamic_cast<LineageBinary &>(*this_data.data);
					if (binary_lineage.right != nullptr) {
						auto right = binary_lineage.right->Backward(key.chunk.inner_simple_agg_idx);
						key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
					}

					if (binary_lineage.left != nullptr) {
						auto left = binary_lineage.left->Backward(key.chunk.inner_simple_agg_idx);
						key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
						key.child_ptrs[left_idx++] = binary_lineage.GetChild();
					}
					out_idx++;
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

				if (dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).right != nullptr) {
					auto right = dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).right->Backward(source);
					key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
				}

				if (dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).left != nullptr) {
					auto left = dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).left->Backward(source);
					key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
					key.child_ptrs[left_idx++] = key.child_ptrs[i]->data->GetChild();
				}
			}
		}
		// Set cardinality of chunks
		key.chunk.SetCardinality(right_idx);
		key.join_chunk.SetCardinality(left_idx);

		// Clear out child pointers for values that didn't have a left match
		for (; left_idx < key.chunk.size(); left_idx++) {
			key.child_ptrs[left_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::CROSS_PRODUCT: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		idx_t out_idx = 0;

		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					key.join_chunk.SetValue(0, out_idx, Value::UBIGINT(this_data.data->data->Backward(this_data.source)));
					key.chunk.SetValue(0, out_idx, Value::UBIGINT(this_data.source));
					key.child_ptrs[out_idx++] = this_data.data->data->GetChild();
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
			key.join_chunk.SetCardinality(out_idx);
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					key.join_chunk.SetValue(0, out_idx, Value::UBIGINT(this_data.data->Backward(key.chunk.inner_simple_agg_idx)));
					key.chunk.SetValue(0, out_idx, Value::UBIGINT(key.chunk.inner_simple_agg_idx));
					key.child_ptrs[out_idx++] = this_data.data->GetChild();
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
			key.join_chunk.SetCardinality(out_idx);
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

				key.join_chunk.SetValue(0, i, Value::UBIGINT(key.child_ptrs[i]->data->Backward(source)));
				key.chunk.SetValue(0, i, Value::UBIGINT(source));
				key.child_ptrs[i] = key.child_ptrs[i]->data->GetChild();
			}

			key.join_chunk.SetCardinality(key.chunk.size());
		}
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		idx_t out_idx = 0;
		idx_t right_idx = 0;
		idx_t left_idx = 0;

		if (!key.chunk.lineage_agg_data->empty()) {
			if (key.chunk.lineage_agg_data->at(0)->at(0).data == nullptr) {
				LookupAggChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
			}
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					LineageBinary &binary_lineage = dynamic_cast<LineageBinary &>(*this_data.data->data);
					if (binary_lineage.right != nullptr) {
						auto right = binary_lineage.right->Backward(this_data.source);
						key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
					}

					if (binary_lineage.left != nullptr) {
						auto left = binary_lineage.left->Backward(this_data.source);
						key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
						key.child_ptrs[left_idx++] = binary_lineage.GetChild();
					}
					out_idx++;
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					LineageBinary &binary_lineage = dynamic_cast<LineageBinary &>(*this_data.data);
					if (binary_lineage.right != nullptr) {
						auto right = binary_lineage.right->Backward(key.chunk.inner_simple_agg_idx);
						key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
					}

					if (binary_lineage.left != nullptr) {
						auto left = binary_lineage.left->Backward(key.chunk.inner_simple_agg_idx);
						key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
						key.child_ptrs[left_idx++] = binary_lineage.GetChild();
					}
					out_idx++;
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
		} else {
			if (key.child_ptrs[0] == nullptr) {
				key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
			}

			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

				if (dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).right != nullptr) {
					auto right = dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).right->Backward(source);
					key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
				}

				if (dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).left != nullptr) {
					auto left = dynamic_cast<LineageBinary &>(*key.child_ptrs[i]->data).left->Backward(source);
					key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
					key.child_ptrs[left_idx++] = key.child_ptrs[i]->data->GetChild();
				}
			}
		}
		// Set cardinality of chunks
		key.chunk.SetCardinality(right_idx);
		key.join_chunk.SetCardinality(left_idx);

		// Clear out child pointers for values that didn't have a left match
		for (; left_idx < key.chunk.size(); left_idx++) {
			key.child_ptrs[left_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
		idx_t out_idx = 0;
		auto data_ptr = make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY][0]);
		if (!key.chunk.lineage_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
				auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
				while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
					key.chunk.SetValue(0, out_idx++, Value::UBIGINT(data_ptr->data->Backward(this_data.source)));
					key.chunk.inner_agg_idx++;
				}
				if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
					break;
				}
				key.chunk.inner_agg_idx = 0;
				key.chunk.outer_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			while (out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
				LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
				while(out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					key.chunk.SetValue(0, out_idx++, Value::UBIGINT(data_ptr->data->Backward(key.chunk.inner_simple_agg_idx)));
					key.chunk.inner_simple_agg_idx++;
				}
				if (key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
					break;
				}
				key.chunk.inner_simple_agg_idx = 0;
				key.chunk.outer_simple_agg_idx++;
			}
			key.chunk.SetCardinality(out_idx);
		} else {
			for (idx_t i = 0; i < key.chunk.size(); i++) {
				key.chunk.SetValue(0, i, Value::UBIGINT(data_ptr->data->Backward(key.chunk.GetValue(0, i).GetValue<uint64_t>())));
			}
			key.child_ptrs = {};
		}
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Recurse until we find a filter-like child, then use all of its lineage
		// This optimizations allows us to skip aggregations and order bys - especially helping for query 15
		key.chunk.next_lineage_simple_agg_data = RecurseForSimpleAgg(children[0]);
		key.child_ptrs = {};
		break;
	}
	default: {
		// We must capture lineage for everything that BACKWARD is called on
		D_ASSERT(false);
	}
	}
}

} // namespace duckdb
#endif
