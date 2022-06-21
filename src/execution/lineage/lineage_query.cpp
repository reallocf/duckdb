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

struct JoinPair {
	idx_t left_idx;
	idx_t right_idx;
};

void GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	bool simple_agg_flag,
	vector<unique_ptr<PhysicalOperator>> *pipelines,
    idx_t number_of_cols,
    vector<JoinPair> *join_pairs // Which pipelines are joined together and in what order
);

unique_ptr<PhysicalOperator> CombineByMode(
	ClientContext &context,
	const string& mode,
	bool should_count,
	vector<unique_ptr<PhysicalOperator>> pipelines,
    vector<JoinPair> join_pairs
);

// Post Processing to prepare for querying

void LineageManager::PostProcess(PhysicalOperator *op, bool should_index) {
	// massage the data to make it easier to query
	bool always_post_process =
		op->type == PhysicalOperatorType::HASH_GROUP_BY || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY;
	bool never_post_process =
		op->type == PhysicalOperatorType::ORDER_BY; // 1 large chunk, so index is useless
	if ((always_post_process) && !never_post_process) {
		vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
		for (idx_t i = 0; i < table_column_types.size(); i++) {
			bool skip_this_sel_vec =
				(op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_COMBINE)
				|| (op->type == PhysicalOperatorType::HASH_JOIN && i == LINEAGE_BUILD)
				|| (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::MARK)
				|| (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_COMBINE)
				|| (op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_SOURCE)
				|| (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_SOURCE);
			if (skip_this_sel_vec) {
				continue;
			}
			// for hash join, build hash table on the build side that map the address to id
			// for group by, build hash table on the unique groups
			for (auto const& lineage_op : op->lineage_op) {
				idx_t chunk_count = 0;
				LineageProcessStruct lps = lineage_op.second->PostProcess(chunk_count, 0, lineage_op.first);
				while (lps.still_processing) {
					lps = lineage_op.second->PostProcess(++chunk_count,  lps.count_so_far, lps.data_idx, lps.finished_idx);
				}
				lineage_op.second->FinishedProcessing(lps.data_idx, lps.finished_idx);
			}
		}
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->children[0].get(), true);
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), true);
		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), true);
		return;
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
			|| (op->type == PhysicalOperatorType::PROJECTION && should_index); // Pass through should_index on projection
		PostProcess(op->children[i].get(), child_should_index);
	}
}

LineageProcessStruct OperatorLineage::PostProcess(idx_t chunk_count, idx_t count_so_far, idx_t data_idx, idx_t finished_idx) {
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
				LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
				idx_t res_count = this_data.data->Count();
//				if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
//					// get min-max on this payload
//					auto min_v = std::numeric_limits<idx_t>::max();
//					auto max_v = std::numeric_limits<idx_t>::min();
//					if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
//						auto payload = (sel_t*)this_data.data->Process(0);
//						for (idx_t i=0; i < res_count; ++i) {
//							if ( (idx_t)payload[i] < min_v) min_v  = (idx_t)payload[i];
//							if ( (idx_t)payload[i] > max_v) max_v  = (idx_t)payload[i];
//						}
//					} else {
//						auto payload = (uint64_t*)this_data.data->Process(0);
//						for (idx_t i=0; i < res_count; ++i) {
//							if ( payload[i] < min_v) min_v  = payload[i];
//							if ( payload[i] > max_v) max_v  = payload[i];
//						}
//					}
//					hm_range.push_back(std::make_pair(min_v, max_v));
//					hash_chunk_count.push_back(count_so_far);
//				} else {
				if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
					auto payload = (sel_t*)this_data.data->Process(0);
					for (idx_t i=0; i < res_count; ++i) {
//							if (hash_map_agg[(idx_t)payload[i]] == nullptr) {
//								hash_map_agg[(idx_t)payload[i]] = make_shared<vector<SourceAndMaybeData>>();
//							}
						hash_map_agg[(idx_t)payload[i]].push_back({i + count_so_far, nullptr});
					}
				} else {
					auto payload = (uint64_t*)this_data.data->Process(0);
					for (idx_t i=0; i < res_count; ++i) {
//							if (hash_map_agg[(idx_t)payload[i]] == nullptr) {
//								hash_map_agg[(idx_t)payload[i]] = make_shared<vector<SourceAndMaybeData>>();
//							}
						hash_map_agg[(idx_t)payload[i]].push_back({i + count_so_far, nullptr});
					}
				}
//				}
				count_so_far += res_count;
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

// TODO can delete?
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
	vector<unique_ptr<PhysicalOperator>> pipelines;
	vector<JoinPair> join_pairs;
	GenerateCustomPlan(op, context, lineage_id, nullptr, false, &pipelines, 1, &join_pairs);
	D_ASSERT(pipelines.size() - 1 == join_pairs.size());

	unique_ptr<PhysicalOperator> final_plan = CombineByMode(context, mode, should_count, move(pipelines), join_pairs);
	return context.RunPlan(final_plan.get());
}

// Lineage Query Plan Constructing

unique_ptr<PhysicalIndexJoin> PreparePhysicalIndexJoin(
    PhysicalOperator *op,
    unique_ptr<PhysicalOperator> left,
    ClientContext &cxt,
    ChunkCollection *chunk_collection,
    idx_t number_of_cols
) {
	auto logical_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
	for (idx_t i = 0; i < number_of_cols; i++) {
		logical_join->types.push_back(LogicalType::UBIGINT);
	}
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

void GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	bool simple_agg_flag,
	vector<unique_ptr<PhysicalOperator>> *pipelines,
    idx_t number_of_cols,
	vector<JoinPair> *join_pairs
) {
	if (!op) {
		pipelines->push_back(move(left));
		return;
	}
	if (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type == JoinType::MARK) {
		// Skip Mark Joins
		GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines, number_of_cols, join_pairs);
		return;
	}
	if (op->type == PhysicalOperatorType::PROJECTION) {
		// Skip Projections
		GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines, number_of_cols, join_pairs);
		return;
	}
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		// Skip DELIM_SCANs since they never affect the lineage
		pipelines->push_back(move(left));
		return;
	}
	if (op->children.empty()) {
		pipelines->push_back(PreparePhysicalIndexJoin(op, move(left), cxt, nullptr, number_of_cols));
		return;
	}
	if (simple_agg_flag && (
		op->type == PhysicalOperatorType::HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::SIMPLE_AGGREGATE ||
		op->type == PhysicalOperatorType::ORDER_BY ||
		op->type == PhysicalOperatorType::PROJECTION
	)) {
		GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), true, pipelines, number_of_cols, join_pairs);
		return;
	}
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
		GenerateCustomPlan(dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines, number_of_cols, join_pairs);
		return;
	}

	vector<LogicalType> types;
	for (idx_t i = 0; i < number_of_cols; i++) {
		types.push_back(LogicalType::UBIGINT);
	}
	vector<LogicalType> build_chunk_types = {LogicalType::UBIGINT, LogicalType::UBIGINT};
	PhysicalOperatorType op_type = PhysicalOperatorType::CHUNK_SCAN;
	idx_t estimated_cardinality = 1;

	if (left == nullptr) {
		D_ASSERT(number_of_cols == 1);

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
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(build_chunk_types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			JoinPair join_pair;
			join_pair.left_idx = pipelines->size(); // Current size will be the index of THIS pipeline
			// Probe side of join
			GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, build_chunk_scan->collection, number_of_cols + 1), false, pipelines, number_of_cols + 1, join_pairs);

			join_pair.right_idx = pipelines->size(); // After inserting custom plan (and any generated by children), now size will be NEXT pipeline
			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines, number_of_cols + 1, join_pairs);
			}

			join_pairs->push_back(join_pair);
		} else {
			return GenerateCustomPlan(
			    op->children[0].get(),
			    cxt,
			    lineage_id,
			    PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, nullptr, number_of_cols),
			    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
			    pipelines,
			    number_of_cols,
			    join_pairs
			);
		}
	} else {
		if (op->children.size() == 2 && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI) {
			types.push_back(LogicalType::UBIGINT);
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			JoinPair join_pair;
			join_pair.left_idx = pipelines->size(); // Current size will be the index of THIS pipeline
			// Probe side of join
			GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(left), cxt,  build_chunk_scan->collection, number_of_cols + 1), false, pipelines, number_of_cols + 1, join_pairs);

			join_pair.right_idx = pipelines->size(); // After inserting custom plan (and any generated by children), now size will be NEXT pipeline
			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines, number_of_cols + 1, join_pairs);
			}

			join_pairs->push_back(join_pair);
		} else {
			GenerateCustomPlan(
			    op->children[0].get(),
			    cxt,
			    lineage_id,
			    PreparePhysicalIndexJoin(op, move(left), cxt, nullptr, number_of_cols),
			    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
			    pipelines,
			    number_of_cols,
			    join_pairs
			);
		}
	}
}

unique_ptr<PhysicalOperator> CombineByMode(
	ClientContext &context,
	const string& mode,
	bool should_count,
	vector<unique_ptr<PhysicalOperator>> pipelines,
    vector<JoinPair> join_pairs
) {
	unique_ptr<PhysicalOperator> final_plan = move(pipelines[0]);
	if (mode == "LIN") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		// Count of Lineage - union then aggregate
		for (idx_t i = 1; i < pipelines.size(); i++) {
			final_plan = make_unique<PhysicalUnion>(
				types,
				move(final_plan),
				move(pipelines[i]),
				1 // TODO improve this?
			);
		}
	} else if (mode == "PERM") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		// TODO this
//		for (idx_t i = 0; i < other_plans.size(); i++) {
//			types.push_back(LogicalType::UBIGINT);
//			final_plan = make_unique<PhysicalCrossProduct>(
//				types,
//				move(other_plans[i]),
//				move(final_plan),
//				1 // TODO improve this?
//			);
//		}
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

SimpleAggQueryStruct OperatorLineage::RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child) {
	vector<LineageDataWithOffset> child_lineage_data_vector;
	switch (child->type) {
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::PROJECTION: {
		return RecurseForSimpleAgg(child->children[0]);
	}
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::INDEX_JOIN: {
		child_lineage_data_vector = child->data[LINEAGE_UNARY];
		break;
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		child_lineage_data_vector = child->data[LINEAGE_PROBE];
		break;
	}
	default:
		// We must capture lineage for everything that RecurseForSimpleAgg is called on
		D_ASSERT(false);
	}

	return {child, child_lineage_data_vector};
}

void OperatorLineage::AccessIndex(LineageIndexStruct key) {
//	std::cout << PhysicalOperatorToString(this->type) << this->opid << std::endl;
//	for (idx_t i = 0; i < key.chunk.size(); i++) {
//		std::cout << key.chunk.GetValue(0,i) << std::endl;
//	}
	if (this->type == PhysicalOperatorType::HASH_JOIN
	|| this->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	|| this->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN
	|| this->type == PhysicalOperatorType::NESTED_LOOP_JOIN
	|| this->type == PhysicalOperatorType::CROSS_PRODUCT
	|| this->type == PhysicalOperatorType::INDEX_JOIN) {
		D_ASSERT(key.chunk.ColumnCount() + 1 == key.col_count);
		// Add column to later join on to key.chunk
		Vector last = Vector(LogicalType::UBIGINT);
		Vector join_last = Vector(LogicalType::UBIGINT);
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			// We add output_so_far so each output from this IndexJoin is assigned a unique id
			last.SetValue(i, Value::UBIGINT(key.output_so_far + i));
			join_last.SetValue(i, Value::UBIGINT(key.output_so_far + i));
		}
		key.chunk.data.push_back(move(last));

		// Initialize join chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT, LogicalType::UBIGINT});
		key.join_chunk.data[1].Reference(join_last);
	}
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
		break;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT: {
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
		}
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();
			idx_t new_val = key.child_ptrs[i]->data->Backward(source);
			key.chunk.SetValue(0, i, Value::UBIGINT(new_val));
			key.child_ptrs[i] = key.child_ptrs[i]->data->GetChild();
		}
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// we need hash table from the build side
		// access the probe side, get the address from the right side
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
		}

		// Replace values in probe chunk and set values in build chunk
		idx_t right_idx = 0;
		idx_t left_idx = 0;
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();
			auto data_index = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).LocateChunkIndex(source);
			auto binary_data = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).GetChunkAt(data_index);
			idx_t adjust_offset = 0;
			if (data_index > 0) {
				// adjust the source
				adjust_offset = dynamic_cast<LineageNested &>(*key.child_ptrs[i]->data).GetAccCount(data_index - 1);
			}
			if (dynamic_cast<LineageBinary&>(*binary_data->data).right != nullptr) {
				key.chunk.SetValue(
				    0,
				    right_idx,
				    Value::UBIGINT(dynamic_cast<LineageBinary &>(*binary_data->data).right->Backward(source - adjust_offset))
				);
				// Copy over other column values
				for (idx_t col_idx = 1; col_idx < key.chunk.ColumnCount(); col_idx++) {
					key.chunk.SetValue(
						col_idx,
					    right_idx,
					    key.chunk.GetValue(col_idx, i)
					);
				}
				key.child_ptrs[right_idx++] = binary_data->data->GetChild();
			}

			if (dynamic_cast<LineageBinary&>(*binary_data->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*binary_data->data).left->Backward(source - adjust_offset);
				if (left == 0) {
					continue;
				}
				if (offset == 0) {
					key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(0));
				} else {
					bool flag = false;
					for (idx_t it = 0; it < hm_range.size();  ++it) {
						if (left >= hm_range[it].first && left <= hm_range[it].second) {
							auto val = ((left - hm_range[it].first) / offset) + hash_chunk_count[it];
							key.join_chunk.SetValue(0, left_idx, Value::UBIGINT(val));
							// Copy over other column value for future joining
							key.join_chunk.SetValue(
							    1,
							    left_idx,
							    key.join_chunk.GetValue(1, i)
							);
							flag = true;
							break;
						}
					}
					D_ASSERT(flag);
				}
				left_idx++;
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
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
		}

		DataChunk orig_chunk;
		orig_chunk.Initialize(orig_chunk.GetTypes());
		key.chunk.Copy(orig_chunk);
		key.chunk.Reset();
		idx_t out_idx = 0;
//		if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
//			for (idx_t i = 0; i < orig_chunk.size(); i++) {
//				auto payload = (uint64_t*)key.child_ptrs[i]->data->Process(0);
//				auto val = payload[orig_chunk.GetValue(0, i).GetValue<uint64_t>()];
//				// iterate the index to find potential chunks
//				auto flag = false;
//				for (idx_t it = 0; it < hm_range.size(); ++it) {
//					if (val >= hm_range[it].first && val <= hm_range[it].second) {
//						// scan this chunk
//						LineageDataWithOffset this_data = data[LINEAGE_SINK][it];
//						idx_t res_count = this_data.data->Count();
//						auto sink_payload = (uint64_t *)this_data.data->Process(0);
//						for (idx_t it2 = 0; it2 < res_count; ++it2) {
//							if (sink_payload[it2] == val) {
//								if (out_idx < STANDARD_VECTOR_SIZE) {
//									key.chunk.SetValue(0, out_idx++, Value::UBIGINT(it2 + hash_chunk_count[it]));
//								} else {
//									if (key.overflow_count % STANDARD_VECTOR_SIZE == 0) {
//										key.cached_values_chunk.emplace_back(LogicalType::UBIGINT);
//									}
//									key.cached_values_chunk[key.overflow_count / STANDARD_VECTOR_SIZE].SetValue(
//										key.overflow_count % STANDARD_VECTOR_SIZE,
//										Value::UBIGINT(it2 + hash_chunk_count[it])
//									);
//									key.overflow_count++;
//								}
//								flag = true;
//								break;
//							}
//						}
//					}
//					if (flag) {
//						break;
//					}
//				}
//			}
//		} else {
			for (idx_t i = 0; i < orig_chunk.size(); i++) {
				auto payload = (uint64_t*)key.child_ptrs[i]->data->Process(0);
				auto res_list = hash_map_agg[payload[orig_chunk.GetValue(0, i).GetValue<uint64_t>()]];
				for (const auto& res : res_list) {
					if (out_idx < STANDARD_VECTOR_SIZE) {
						key.chunk.SetValue(0, out_idx, Value::UBIGINT(res.source)); // TODO stop using SourceAndMaybeData here
					    for (idx_t col_count = 1; col_count < key.chunk.ColumnCount(); col_count++) {
						    key.chunk.SetValue(col_count, out_idx, orig_chunk.GetValue(col_count, i));
					    }
					    out_idx++;
					} else {
						idx_t chunk_idx = key.overflow_count / STANDARD_VECTOR_SIZE;
						idx_t offset_in_chunk = key.overflow_count % STANDARD_VECTOR_SIZE;
						if (offset_in_chunk == 0) {
						    vector<Vector> new_cached_values_chunk;
						    for (const auto &typ : orig_chunk.GetTypes()) {
							    new_cached_values_chunk.emplace_back(typ);
						    }
							key.cached_values_chunk.push_back(new_cached_values_chunk);
						}
						key.cached_values_chunk[chunk_idx][0].SetValue(offset_in_chunk, Value::UBIGINT(res.source));
					    for (idx_t col_count = 1; col_count < key.chunk.ColumnCount(); col_count++) {
						    key.cached_values_chunk[chunk_idx][col_count].SetValue(offset_in_chunk, orig_chunk.GetValue(col_count, i));
					    }
						key.overflow_count++;
					}
				}
//			}
		}
		key.chunk.SetCardinality(out_idx);
		key.child_ptrs = {};
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_SOURCE], index);
		}

		DataChunk orig_chunk;
		orig_chunk.Initialize(orig_chunk.GetTypes());
		key.chunk.Copy(orig_chunk);
		key.chunk.Reset();
		idx_t out_idx = 0;
//		if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
//			for (idx_t i = 0; i < orig_chunk.size(); i++) {
//				auto payload = (sel_t*)key.child_ptrs[i]->data->Process(0);
//				auto val = payload[orig_chunk.GetValue(0, i).GetValue<uint64_t>()];
//				// iterate the index to find potential chunks
//				auto flag = false;
//				for (idx_t it = 0; it < hm_range.size(); ++it) {
//					if (val >= hm_range[it].first && val <= hm_range[it].second) {
//						// scan this chunk
//						LineageDataWithOffset this_data = data[LINEAGE_SINK][it];
//						idx_t res_count = this_data.data->Count();
//						auto sink_payload = (sel_t *)this_data.data->Process(0);
//						for (idx_t it2 = 0; it2 < res_count; ++it2) {
//							if (sink_payload[it2] == val) {
//								if (out_idx < STANDARD_VECTOR_SIZE) {
//									key.chunk.SetValue(0, out_idx++, Value::UBIGINT(it2 + hash_chunk_count[it]));
//								} else {
//									if (key.overflow_count % STANDARD_VECTOR_SIZE == 0) {
//										key.cached_values_chunk.emplace_back(LogicalType::UBIGINT);
//									}
//									key.cached_values_chunk[key.overflow_count / STANDARD_VECTOR_SIZE].SetValue(
//										key.overflow_count % STANDARD_VECTOR_SIZE,
//										Value::UBIGINT(it2 + hash_chunk_count[it])
//									);
//									key.overflow_count++;
//								}
//								flag = true;
//								break;
//							}
//						}
//					}
//					if (flag) {
//						break;
//					}
//				}
//			}
//		} else {
			for (idx_t i = 0; i < orig_chunk.size(); i++) {
				auto payload = (sel_t*)key.child_ptrs[i]->data->Process(0);
				auto res_list = hash_map_agg[payload[orig_chunk.GetValue(0, i).GetValue<uint64_t>()]];
				for (const auto& res : res_list) {
					if (out_idx < STANDARD_VECTOR_SIZE) {
						key.chunk.SetValue(0, out_idx, Value::UBIGINT(res.source));
						for (idx_t col_count = 1; col_count < key.chunk.ColumnCount(); col_count++) {
							key.chunk.SetValue(col_count, out_idx, orig_chunk.GetValue(col_count, i));
						}
						out_idx++;
					} else {
						idx_t chunk_idx = key.overflow_count / STANDARD_VECTOR_SIZE;
						idx_t offset_in_chunk = key.overflow_count % STANDARD_VECTOR_SIZE;
						if (offset_in_chunk == 0) {
							vector<Vector> new_cached_values_chunk;
							for (const auto &typ : orig_chunk.GetTypes()) {
								new_cached_values_chunk.emplace_back(typ);
							}
							key.cached_values_chunk.push_back(new_cached_values_chunk);
						}
						key.cached_values_chunk[chunk_idx][0].SetValue(offset_in_chunk, Value::UBIGINT(res.source));
						for (idx_t col_count = 1; col_count < key.chunk.ColumnCount(); col_count++) {
							key.cached_values_chunk[chunk_idx][col_count].SetValue(offset_in_chunk, orig_chunk.GetValue(col_count, i));
						}
						key.overflow_count++;
					}
				}
			}
//		}
		key.chunk.SetCardinality(out_idx);
		key.child_ptrs = {};
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
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
		}

		idx_t right_idx = 0;
		idx_t left_idx = 0;
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

			if (dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).right != nullptr) {
				auto right = dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).right->Backward(source);
				key.join_chunk.SetValue(0, right_idx++, Value::UBIGINT(right));
				// Copy over other column value for future joining
				key.join_chunk.SetValue(
					1,
					right_idx,
					key.join_chunk.GetValue(1, i)
				);
			}

			if (dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).left->Backward(source);
				key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
				// Copy over other column values
				for (idx_t col_idx = 1; col_idx < key.chunk.ColumnCount(); col_idx++) {
					key.chunk.SetValue(
						col_idx,
						left_idx,
						key.chunk.GetValue(col_idx, i)
					);
				}
				key.child_ptrs[left_idx++] = key.child_ptrs[i]->data->GetChild();
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
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_PROBE], index);
		}

		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

			key.join_chunk.SetValue(0, i, Value::UBIGINT(key.child_ptrs[i]->data->Backward(source)));
			key.chunk.SetValue(0, i, Value::UBIGINT(source));
			// No need to copy other column values - they can stay as-is
			key.child_ptrs[i] = key.child_ptrs[i]->data->GetChild();
		}
		key.join_chunk.SetCardinality(key.chunk.size());
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		if (key.child_ptrs[0] == nullptr) {
			key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, data[LINEAGE_UNARY], index);
		}

		idx_t right_idx = 0;
		idx_t left_idx = 0;
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t source = key.chunk.GetValue(0, i).GetValue<uint64_t>();

			if (dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).right != nullptr) {
				auto right = dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).right->Backward(source);
				key.join_chunk.SetValue(0, right_idx, Value::UBIGINT(right));
				// Copy over other column value for future joining
				key.join_chunk.SetValue(
					1,
					right_idx,
					key.join_chunk.GetValue(1, i)
				);
				right_idx++;
			}

			if (dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*key.child_ptrs[i]->data).left->Backward(source);
				key.chunk.SetValue(0, left_idx, Value::UBIGINT(left));
				for (idx_t col_idx = 1; col_idx < key.chunk.ColumnCount(); col_idx++) {
					key.chunk.SetValue(
						col_idx,
						left_idx,
						key.chunk.GetValue(col_idx, i)
					);
				}
				key.child_ptrs[left_idx] = key.child_ptrs[i]->data->GetChild();
				left_idx++;
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
		auto data_ptr = make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY][0]);
		for (idx_t i = 0; i < key.chunk.size(); i++) {
			idx_t new_val = data_ptr->data->Backward(key.chunk.GetValue(0, i).GetValue<uint64_t>());
			key.chunk.SetValue(0, i, Value::UBIGINT(new_val));
		}
		key.child_ptrs = {};
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Recurse until we find a filter-like child, then use all of its lineage
		// This optimizations allows us to skip aggregations and order bys - especially helping for query 15
		SimpleAggQueryStruct agg_struct = RecurseForSimpleAgg(children[0]);

		idx_t out_idx = 0;
		LineageDataWithOffset final_child = agg_struct.child_lineage_data_vector[agg_struct.child_lineage_data_vector.size() - 1];
		idx_t child_total_size = final_child.this_offset + final_child.data->Count();
		for (const LineageDataWithOffset& child_lineage_data : agg_struct.child_lineage_data_vector) {
			for (idx_t i = 0; i < child_lineage_data.data->Count(); i++) {
				if (out_idx < STANDARD_VECTOR_SIZE) {
					key.chunk.SetValue(0, out_idx, Value::UBIGINT(i));
					key.child_ptrs[out_idx++] = make_shared<LineageDataWithOffset>(child_lineage_data);
				} else {
					// In Simple Aggregate, we'll always cross join these values since they'll all match with each one
					// so we don't need to build a larger cached_values_chunk
					idx_t chunk_idx = key.overflow_count / STANDARD_VECTOR_SIZE;
					idx_t offset_in_chunk = key.overflow_count % STANDARD_VECTOR_SIZE;
					if (offset_in_chunk == 0) {
						key.cached_values_chunk[0].emplace_back(LogicalType::UBIGINT);
						key.cached_child_ptrs_arr.emplace_back();
						key.cached_child_ptrs_arr[chunk_idx].reserve(
						    child_total_size - key.overflow_count > STANDARD_VECTOR_SIZE ?
						    	STANDARD_VECTOR_SIZE :
						        child_total_size - key.overflow_count
						);
					}
					key.cached_values_chunk[0][chunk_idx].SetValue(offset_in_chunk, Value::UBIGINT(i));
					key.cached_child_ptrs_arr[chunk_idx].push_back(make_shared<LineageDataWithOffset>(child_lineage_data));
					key.overflow_count++;
				}
			}
		}
		key.chunk.SetCardinality(out_idx < STANDARD_VECTOR_SIZE ? out_idx : STANDARD_VECTOR_SIZE);
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
