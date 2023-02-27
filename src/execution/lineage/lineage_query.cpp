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
	bool simple_agg_flag,
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

void LineageManager::PostProcess(PhysicalOperator *op) {
	// massage the data to make it easier to query
	if (op->type == PhysicalOperatorType::HASH_GROUP_BY || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
		// for group by, build hash table on the unique groups
		for (auto const& lineage_op : *op->lineage_op) {
			if (lineage_op.second->type != PhysicalOperatorType::HASH_GROUP_BY && lineage_op.second->type != PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
				continue;
			}

			lineage_op.second->PostProcess();
		}
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->children[0].get());
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get());
		return;
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		PostProcess(op->children[i].get());
	}
}

void OperatorLineage::PostProcess() {
	 std::cout << "Postprocess: " << PhysicalOperatorToString(this->type) << this->opid << std::endl;
	// build hash table
	idx_t count_so_far = 0;
	for (idx_t i = 0; i < data[LINEAGE_SINK]->size(); i++) {
		LineageDataWithOffset this_data = (*data[LINEAGE_SINK])[i];
		idx_t res_count = this_data.data->Count();
		if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
			auto payload = (sel_t *)this_data.data->Process(0);
			for (idx_t j = 0; j < res_count; ++j) {
				auto bucket = (idx_t)payload[j];
				if ((*hash_map_agg)[bucket] == nullptr) {
					(*hash_map_agg)[bucket] = make_shared<vector<SourceAndMaybeData>>();
				}

				auto child = this_data.data->GetChild();
				// We capture global value, so we convert to child local value here
				auto val = j + count_so_far - child->this_offset;
				(*hash_map_agg)[bucket]->push_back({val, child});
			}
		} else if (type == PhysicalOperatorType::HASH_GROUP_BY) {
			auto payload = (uint64_t *)this_data.data->Process(0);
			for (idx_t j = 0; j < res_count; ++j) {
				auto bucket = (idx_t)payload[j];
				if ((*hash_map_agg)[bucket] == nullptr) {
					(*hash_map_agg)[bucket] = make_shared<vector<SourceAndMaybeData>>();
				}

				auto child = this_data.data->GetChild();
				// We capture global value, so we convert to child local value here
				auto val = j + count_so_far - child->this_offset;
				(*hash_map_agg)[bucket]->push_back({val, child});
			}
		} else {
			// Invalid post process - should only be aggregations
			throw std::logic_error("Only should be called for group by");
		}
		count_so_far += res_count;
	}
	std::cout << "Finished Postprocessing!" << std::endl;
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
	unique_ptr<PhysicalOperator> first_plan = GenerateCustomPlan(op, context, lineage_id, nullptr, false, &other_plans);
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
	unique_ptr<Index> lineage_index = make_unique<LineageIndex>(cids, exps, op->lineage_op->at(-1));
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
		op->lineage_op->at(-1),
		chunk_collection
	);
}

unique_ptr<PhysicalOperator> GenerateCustomPlan(
	PhysicalOperator* op,
	ClientContext &cxt,
	int lineage_id,
	unique_ptr<PhysicalOperator> left,
	bool simple_agg_flag,
	vector<unique_ptr<PhysicalOperator>> *pipelines
) {
	if (!op) {
		return left;
	}
	if (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type == JoinType::MARK) {
		// Skip Mark Joins
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
	}
	if (op->type == PhysicalOperatorType::PROJECTION) {
		// Skip Projections
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
	}
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		// Skip DELIM_SCANs since they never affect the lineage
		return left;
	}
	if (op->children.empty()) {
		return PreparePhysicalIndexJoin(op, move(left), cxt, nullptr);
	}
	if (simple_agg_flag && (
		op->type == PhysicalOperatorType::HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::SIMPLE_AGGREGATE ||
		op->type == PhysicalOperatorType::ORDER_BY ||
		op->type == PhysicalOperatorType::PROJECTION
	)) {
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), true, pipelines);
	}
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
			op->lineage_op->at(thread_id)->children[2]->children.push_back(op->lineage_op->at(thread_id)->children[0]);

			// chunk scan input is delim join input
			op->lineage_op->at(thread_id)->children[1]->children[1] = op->lineage_op->at(thread_id)->children[0];

			// we only want to do the child reordering once
			op->delim_handled = true;
		}
		return GenerateCustomPlan(dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
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
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, build_chunk_scan->collection), false, pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines));
			}

			return custom_plan;
		}
		return GenerateCustomPlan(
			op->children[0].get(),
			cxt,
			lineage_id,
			PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, nullptr),
			op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
			pipelines
		);
	} else {
		if (op->children.size() == 2 && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(left), cxt,  build_chunk_scan->collection), false, pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines));
			}

			// probe side of hash join
			return custom_plan;
		}
		return GenerateCustomPlan(
			op->children[0].get(),
			cxt,
			lineage_id,
			PreparePhysicalIndexJoin(op, move(left), cxt, nullptr),
			op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
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

vector<shared_ptr<LineageDataWithOffset>> OperatorLineage::LookupChunksFromGlobalIndex(
    DataChunk &chunk,
    idx_t lineage_idx
) {
	vector<shared_ptr<LineageDataWithOffset>> res;
	res.reserve(chunk.size());
	// Binary Search index
	for (idx_t i = 0; i < chunk.size(); i++) {
		idx_t val = chunk.GetValue(0, i).GetValue<uint64_t>();
		// we need a way to locate the exact data we should access
		// from the source index
		auto lower = lower_bound(index->begin(), index->end(), val);
		if (lower == index->end() || (lower == index->end() - 1 && *lower == val)) {
			throw std::logic_error("Out of bounds lineage requested");
		}
		auto chunk_id = lower - index->begin();
		if (*lower == val) {
			chunk_id += 1;
		}
		auto this_data = data[lineage_idx]->at(chunk_id);
		if (chunk_id > 0) {
			val -= index->at(chunk_id - 1);
		}
		chunk.SetValue(0, i, Value::UBIGINT(val));
		res.push_back(make_unique<LineageDataWithOffset>(this_data));
	}
	return res;
}

shared_ptr<vector<LineageDataWithOffset>> OperatorLineage::RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child) {
	shared_ptr<vector<LineageDataWithOffset>> child_lineage_data_vector;
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

	return child_lineage_data_vector;
}

void OperatorLineage::ScanLineageFunc(idx_t source, const shared_ptr<LineageDataWithOffset>& lineage_data,
                                      vector<shared_ptr<idx_t>> idxs, LineageIndexStruct key) {
	idx_t val = lineage_data->data->Backward(source) + lineage_data->child_offset;
	key.chunk.SetValue(0, (*idxs.at(0))++, Value::UBIGINT(val));
}

void OperatorLineage::FilterLimitLineageFunc(idx_t source, const shared_ptr<LineageDataWithOffset>& lineage_data,
                                             vector<shared_ptr<idx_t>> idxs, LineageIndexStruct key) {
	idx_t new_val = lineage_data->data->Backward(source);
	key.chunk.SetValue(0, *idxs.at(0), Value::UBIGINT(new_val));
	key.child_ptrs[(*idxs.at(0))++] = lineage_data->data->GetChild();
}

void OperatorLineage::HashJoinLineageFunc(
    idx_t source,
    const shared_ptr<LineageDataWithOffset>& lineage_data,
    vector<shared_ptr<idx_t>> idxs,
    LineageIndexStruct key
) {
	LineageNested &nested_lineage = dynamic_cast<LineageNested &>(*lineage_data->data);
	shared_ptr<idx_t> left_i = idxs[1];
	shared_ptr<idx_t> right_i = idxs[2];
	int data_index = nested_lineage.LocateChunkIndex(source);
	LineageBinary& binary_data = dynamic_cast<LineageBinary &>(*(nested_lineage.GetChunkAt(data_index))->data);
	idx_t adjust_offset = 0;
	if (data_index > 0) {
		// adjust the source
		adjust_offset = nested_lineage.GetAccCount(data_index - 1);
	}
	if (binary_data.right != nullptr) {
		key.chunk.SetValue(0,*right_i,Value::UBIGINT(binary_data.right->Backward(source - adjust_offset)));
		key.child_ptrs[*right_i] = binary_data.GetChild();
		(*right_i)++;
	}

	if (binary_data.left != nullptr) {
		auto left = binary_data.left->Backward(source - adjust_offset);
		if (left == 0) {
			return;
		}
		if (offset == 0) {
			key.join_chunk.SetValue(0, *left_i, Value::UBIGINT(0));
		} else {
			bool flag = false;
			for (idx_t it = 0; it < hm_range->size(); ++it) {
				if (left >= hm_range->at(it).first && left <= hm_range->at(it).second) {
					auto val = ((left - hm_range->at(it).first) / offset) + hash_chunk_count->at(it);
					key.join_chunk.SetValue(0, *left_i, Value::UBIGINT(val));
					flag = true;
					break;
				}
			}
			D_ASSERT(flag);
		}
		(*left_i)++;
	}
	(*idxs[0])++;
}

void OperatorLineage::HashAggLineageFunc(
    idx_t source,
    const shared_ptr<LineageDataWithOffset>& lineage_data,
    vector<shared_ptr<idx_t>> idxs,
    LineageIndexStruct key
) {
	auto payload = (uint64_t *)lineage_data->data->Process(0);
	key.chunk.next_lineage_agg_data->push_back((*hash_map_agg)[payload[source]]);
	(*idxs[0])++;
	key.child_ptrs = {};
}

void OperatorLineage::PerfectHashAggLineageFunc(
    idx_t source,
    const shared_ptr<LineageDataWithOffset>& lineage_data,
    vector<shared_ptr<idx_t>> idxs,
    LineageIndexStruct key
) {
	auto payload = (sel_t *)lineage_data->data->Process(0);
	key.chunk.next_lineage_agg_data->push_back((*hash_map_agg)[payload[source]]);
	(*idxs[0])++;
	key.child_ptrs = {};
}

void OperatorLineage::BlockwiseIndexNLPiecewiseJoinsLineageFunc(
    idx_t source,
    const shared_ptr<LineageDataWithOffset>& lineage_data,
    vector<shared_ptr<idx_t>> idxs,
    LineageIndexStruct key
) {
	LineageBinary &binary_lineage = dynamic_cast<LineageBinary &>(*lineage_data->data);
	shared_ptr<idx_t> left_i = idxs[1];
	shared_ptr<idx_t> right_i = idxs[2];
	if (binary_lineage.right != nullptr) {
		auto right = binary_lineage.right->Backward(source);
		key.join_chunk.SetValue(0, (*right_i)++, Value::UBIGINT(right));
	}

	if (binary_lineage.left != nullptr) {
		auto left = binary_lineage.left->Backward(source);
		key.chunk.SetValue(0, *left_i, Value::UBIGINT(left));
		key.child_ptrs[(*left_i)++] = binary_lineage.GetChild();
	}
	(*idxs[0])++;
}

void OperatorLineage::CrossProductLineageFunc(idx_t source, const shared_ptr<LineageDataWithOffset>& lineage_data,
                                              vector<shared_ptr<idx_t>> idxs, LineageIndexStruct key) {
	key.join_chunk.SetValue(0, *idxs.at(0), Value::UBIGINT(lineage_data->data->Backward(source)));
	key.chunk.SetValue(0, *idxs.at(0), Value::UBIGINT(source));
	key.child_ptrs[(*idxs.at(0))++] = lineage_data->data->GetChild();
}

void OperatorLineage::OrderByLineageFunc(idx_t source, const shared_ptr<LineageDataWithOffset>& lineage_data,
                                         vector<shared_ptr<idx_t>> idxs, LineageIndexStruct key) {
	idx_t new_val = lineage_data->data->Backward(source);
	key.chunk.SetValue(0, (*idxs.at(0))++, Value::UBIGINT(new_val));
}

void OperatorLineage::AggIterate(LineageIndexStruct key, vector<shared_ptr<idx_t>> idxs) {
	shared_ptr<idx_t> out_idx = idxs[0];
	while(*out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_agg_idx < key.chunk.lineage_agg_data->size()) {
		auto agg_vec_ptr = key.chunk.lineage_agg_data->at(key.chunk.outer_agg_idx);
		while(*out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
			auto this_data = agg_vec_ptr->at(key.chunk.inner_agg_idx);
			switch (this->type) {
			case PhysicalOperatorType::TABLE_SCAN: {
				ScanLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::FILTER:
			case PhysicalOperatorType::LIMIT: {
				FilterLimitLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::HASH_JOIN: {
				HashJoinLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::HASH_GROUP_BY: {
				HashAggLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
				PerfectHashAggLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
			case PhysicalOperatorType::INDEX_JOIN:
			case PhysicalOperatorType::NESTED_LOOP_JOIN:
			case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
				BlockwiseIndexNLPiecewiseJoinsLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::CROSS_PRODUCT: {
				CrossProductLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			case PhysicalOperatorType::ORDER_BY: {
				OrderByLineageFunc(this_data.source, this_data.data, idxs, key);
				break;
			}
			default: {
				throw std::logic_error("Unexpected op passed to AggIterate " + PhysicalOperatorToString(this->type));
			}
			}
			key.chunk.inner_agg_idx++;
		}
		if (key.chunk.inner_agg_idx < agg_vec_ptr->size()) {
			break;
		}
		key.chunk.inner_agg_idx = 0;
		key.chunk.outer_agg_idx++;
	}
}

void OperatorLineage::NormalIterate(LineageIndexStruct key, vector<shared_ptr<idx_t>> idxs, idx_t lineage_idx) {
	shared_ptr<idx_t> out_idx = idxs[0];
	if (key.child_ptrs[0] == nullptr) {
		key.child_ptrs = LookupChunksFromGlobalIndex(key.chunk, lineage_idx);
	}
	while (*out_idx < key.chunk.size()) {
		idx_t source = key.chunk.GetValue(0, *out_idx).GetValue<uint64_t>();
		switch (this->type) {
		case PhysicalOperatorType::TABLE_SCAN: {
			ScanLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT: {
			FilterLimitLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			HashJoinLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY: {
			HashAggLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			PerfectHashAggLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::INDEX_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
			BlockwiseIndexNLPiecewiseJoinsLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT: {
			CrossProductLineageFunc(source, key.child_ptrs[*out_idx], idxs, key);
			break;
		}
		default: {
			throw std::logic_error("Unexpected op passed to NormalIterate " + PhysicalOperatorToString(this->type));
		}
		}
	}
}

void OperatorLineage::SimpleAggIterate(LineageIndexStruct key, vector<shared_ptr<idx_t>> idxs) {
	shared_ptr<idx_t> out_idx = idxs[0];
	while (*out_idx < STANDARD_VECTOR_SIZE && key.chunk.outer_simple_agg_idx < key.chunk.lineage_simple_agg_data->size()) {
		LineageDataWithOffset this_data = key.chunk.lineage_simple_agg_data->at(key.chunk.outer_simple_agg_idx);
		while(*out_idx < STANDARD_VECTOR_SIZE && key.chunk.inner_simple_agg_idx < this_data.data->Count()) {
			switch (this->type) {
			case PhysicalOperatorType::TABLE_SCAN: {
				ScanLineageFunc(key.chunk.inner_simple_agg_idx, make_shared<LineageDataWithOffset>(this_data), idxs, key);
				break;
			}
			case PhysicalOperatorType::FILTER:
			case PhysicalOperatorType::LIMIT: {
				FilterLimitLineageFunc(key.chunk.inner_simple_agg_idx, make_shared<LineageDataWithOffset>(this_data), idxs, key);
				break;
			}
			case PhysicalOperatorType::HASH_JOIN: {
				HashJoinLineageFunc(key.chunk.inner_simple_agg_idx, make_shared<LineageDataWithOffset>(this_data), idxs, key);
				break;
			}
			case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
			case PhysicalOperatorType::INDEX_JOIN:
			case PhysicalOperatorType::NESTED_LOOP_JOIN:
			case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
				BlockwiseIndexNLPiecewiseJoinsLineageFunc(key.chunk.inner_simple_agg_idx, make_shared<LineageDataWithOffset>(this_data), idxs, key);
				break;
			}
			case PhysicalOperatorType::CROSS_PRODUCT: {
				CrossProductLineageFunc(key.chunk.inner_simple_agg_idx, make_shared<LineageDataWithOffset>(this_data), idxs, key);
				break;
			}
			default: {
				throw std::logic_error("Unexpected op passed to SimpleAggIterate " + PhysicalOperatorToString(this->type));
			}
			}
			key.chunk.inner_simple_agg_idx++;
		}
		if (key.chunk.inner_agg_idx < this_data.data->Count()) {
			break;
		}
		key.chunk.inner_agg_idx = 0;
		key.chunk.outer_simple_agg_idx++;
	}
}

void OperatorLineage::AccessIndex(LineageIndexStruct key) {
	std::cout << PhysicalOperatorToString(this->type) << this->opid << std::endl;
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
	case PhysicalOperatorType::PROJECTION: {
		// These should have been removed from the query plan
		D_ASSERT(false);
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		if (key.chunk.lineage_agg_data->empty() && data[LINEAGE_UNARY]->empty() && key.child_ptrs[0] == nullptr) {
			// Nothing to do! Lineage correct as-is
		} else {
			shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);
			if (!key.chunk.lineage_agg_data->empty()) {
				AggIterate(key, {out_idx});
			} else if (!key.chunk.lineage_simple_agg_data->empty()) {
				SimpleAggIterate(key, {out_idx});
			} else {
				NormalIterate(key, {out_idx}, LINEAGE_UNARY);
			}
			key.chunk.SetCardinality(*out_idx);
		}
		break;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT: {
		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);
		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx});
		} else {
			NormalIterate(key, {out_idx}, LINEAGE_UNARY);
		}
		key.chunk.SetCardinality(*out_idx);
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> right_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> left_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx, right_idx, left_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx, right_idx, left_idx});
		} else {
			NormalIterate(key, {out_idx, right_idx, left_idx}, LINEAGE_PROBE);
		}

		// Set cardinality of chunks
		key.chunk.SetCardinality(*right_idx);
		key.join_chunk.SetCardinality(*left_idx);

		// Clear out child pointers for values that didn't have a right match
		for (; *right_idx < key.chunk.size(); (*right_idx)++) {
			key.child_ptrs[*right_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		key.chunk.next_lineage_agg_data->reserve(key.chunk.size());

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx});
		} else {
			NormalIterate(key, {out_idx}, LINEAGE_SOURCE);
		}
		// TODO: we never set Cardinality on chunk because we're actually returning more than 1024 values - is this okay?
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		key.chunk.next_lineage_agg_data->reserve(key.chunk.size());

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx});
		} else {
			NormalIterate(key, {out_idx}, LINEAGE_SOURCE);
		}
		// TODO: we never set Cardinality on chunk because we're actually returning more than 1024 values - is this okay?
		break;
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> right_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> left_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx, right_idx, left_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx, right_idx, left_idx});
		} else {
			NormalIterate(key, {out_idx, right_idx, left_idx}, LINEAGE_PROBE);
		}
		// Set cardinality of chunks
		key.chunk.SetCardinality(*right_idx);
		key.join_chunk.SetCardinality(*left_idx);

		// Clear out child pointers for values that didn't have a left match
		for (; *left_idx < key.chunk.size(); (*left_idx)++) {
			key.child_ptrs[*left_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::CROSS_PRODUCT: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});
		key.join_chunk.SetCardinality(key.chunk.size());

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx});
		} else {
			NormalIterate(key, {out_idx}, LINEAGE_PROBE);
		}
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// Setup build chunk
		key.join_chunk.Initialize({LogicalType::UBIGINT});

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> right_idx = make_shared<idx_t>(0);
		shared_ptr<idx_t> left_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx, right_idx, left_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx, right_idx, left_idx});
		} else {
			NormalIterate(key, {out_idx, right_idx, left_idx}, LINEAGE_UNARY);
		}

		// Set cardinality of chunks
		key.chunk.SetCardinality(*right_idx);
		key.join_chunk.SetCardinality(*left_idx);

		// Clear out child pointers for values that didn't have a left match
		for (; *left_idx < key.chunk.size(); (*left_idx)++) {
			key.child_ptrs[*left_idx] = nullptr;
		}
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
		auto data_ptr = make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY]->at(0));

		shared_ptr<idx_t> out_idx = make_shared<idx_t>(0);

		if (!key.chunk.lineage_agg_data->empty()) {
			AggIterate(key, {out_idx});
		} else if (!key.chunk.lineage_simple_agg_data->empty()) {
			SimpleAggIterate(key, {out_idx});
		} else {
			// Handle this directly since we don't ever want to look up by index TODO: clean this up?
			for (idx_t i = 0; i < key.chunk.size(); i++) {
				idx_t new_val = data_ptr->data->Backward(key.chunk.GetValue(0, i).GetValue<uint64_t>());
				key.chunk.SetValue(0, i, Value::UBIGINT(new_val));
			}
		}

		key.child_ptrs = {};
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
