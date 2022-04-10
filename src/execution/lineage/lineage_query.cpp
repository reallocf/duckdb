#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

#include <utility>

#define PROBE_SIZE 10

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;

void LineageManager::PostProcess(PhysicalOperator *op, bool should_index) {
	// massage the data to make it easier to query
	bool always_post_process =
	    op->type == PhysicalOperatorType::HASH_GROUP_BY || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY;
	bool never_post_process =
	    op->type == PhysicalOperatorType::ORDER_BY; // 1 large chunk, so index is useless
	if ((always_post_process || (should_index && LINEAGE_INDEX_TYPE == 1)) && !never_post_process) {
		vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
		for (idx_t i = 0; i < table_column_types.size(); i++) {
			bool skip_this_sel_vec =
				(op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_COMBINE)
			    || (op->type == PhysicalOperatorType::HASH_JOIN && i == LINEAGE_BUILD)
			    || (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::MARK)
			    || (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_COMBINE)
			    || (op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_SOURCE && !(should_index && LINEAGE_INDEX_TYPE == 1))
			    || (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_SOURCE && !(should_index && LINEAGE_INDEX_TYPE == 1));
			if (skip_this_sel_vec) {
				continue;
			}
			// for hash join, build hash table on the build side that map the address to id
			// for group by, build hash table on the unique groups
			for (auto const& lineage_op : op->lineage_op) {
				idx_t chunk_count = 0;
				LineageProcessStruct lps = lineage_op.second->PostProcess(chunk_count, 0, lineage_op.first);
				while (lps.still_processing) {
					lps = lineage_op.second->PostProcess(++chunk_count,  lps.count_so_far, lineage_op.first);
				}
				lineage_op.second->FinishedProcessing();
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


LineageProcessStruct OperatorLineage::PostProcess(idx_t chunk_count, idx_t count_so_far, int thread_id) {
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
				if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
					// get min-max on this payload
					auto min_v = std::numeric_limits<idx_t>::max();
					auto max_v = std::numeric_limits<idx_t>::min();
					if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
						auto payload = (sel_t*)this_data.data->Process(0);
						for (idx_t i=0; i < res_count; ++i) {
							if ( (idx_t)payload[i] < min_v) min_v  = (idx_t)payload[i];
							if ( (idx_t)payload[i] > max_v) max_v  = (idx_t)payload[i];
						}
					} else {
						auto payload = (uint64_t*)this_data.data->Process(0);
						for (idx_t i=0; i < res_count; ++i) {
							if ( payload[i] < min_v) min_v  = payload[i];
							if ( payload[i] > max_v) max_v  = payload[i];
						}
					}
					hm_range.push_back(std::make_pair(min_v, max_v));
					hash_chunk_count.push_back(count_so_far);
				} else {
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
				}
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
	return LineageProcessStruct{ count_so_far, 0, data[finished_idx].size() > data_idx };
}

void AccessLineageDataViaIndex(
    const shared_ptr<vector<SourceAndMaybeData>>& lineage, // We believe that there is no data in any of these if it's passed here
    const vector<LineageDataWithOffset>& data,
    const vector<idx_t>& index
) {
	if (LINEAGE_INDEX_TYPE == 0) {
		// Binary Search index
		for (idx_t i = 0; i < lineage->size(); i++) {
			// we need a way to locate the exact data we should access
			// from the source index
			auto lower = lower_bound(index.begin(), index.end(), (*lineage.get())[i].source);
			if (lower == index.end()) {
				throw std::logic_error("Out of bounds lineage requested");
			}
			auto chunk_id = lower - index.begin();
			if (*lower == (*lineage.get())[i].source) {
				chunk_id += 1;
			}
			auto this_data = data[chunk_id];
			if (chunk_id > 0) {
				(*lineage.get())[i].source -= index[chunk_id-1];
			}
			(*lineage.get())[i].data = make_unique<LineageDataWithOffset>(this_data);
		}
	} else {
		// Array index
		for (idx_t i = 0; i < lineage->size(); i++) {
			idx_t chunk_id = index[(*lineage.get())[i].source];
			auto this_data = data[chunk_id];
			(*lineage.get())[i] = {
				(*lineage.get())[i].source - this_data.this_offset,
				make_unique<LineageDataWithOffset>(this_data)
			};
		}
	}
}

SimpleAggQueryStruct OperatorLineage::RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child) {
	vector<LineageDataWithOffset> child_lineage_data_vector;
	switch (child->type) {
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
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

Generator<unique_ptr<LineageRes>> GenWrapper(unique_ptr<LineageRes> res) {
	co_yield move(res);
}

Generator<unique_ptr<LineageRes>> OperatorLineage::Backward(shared_ptr<vector<SourceAndMaybeData>> lineage) {
	if (lineage->empty()) {
		// Skip if empty
		co_yield make_unique<LineageResVal>(LineageResVal({}));
		co_return;
	}
	switch (this->type) {
	case (PhysicalOperatorType::DELIM_JOIN): {
		// distinct input is delim join input
		// distinct should be the input to delim scan
		children[2]->children.push_back(children[0]);

		// chunk scan input is delim join input
		children[1]->children[1] = children[0];
		Generator<unique_ptr<LineageRes>> child_gen = children[1]->Backward(move(lineage));
		while (child_gen.Next()) {
			co_yield child_gen.GetValue();
		}
		co_return;
	}
	case PhysicalOperatorType::DELIM_SCAN: {
		co_yield make_unique<LineageResVal>(LineageResVal(move(lineage)));
		co_return;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		// End of the recursion!
		if (data[LINEAGE_UNARY].empty() && (*lineage.get())[0].data == nullptr) {
			// Nothing to do! Lineage correct as-is
			co_yield make_unique<LineageResVal>(LineageResVal(move(lineage)));
		} else {
			if ((*lineage.get())[0].data == nullptr) {
				AccessLineageDataViaIndex(lineage, data[LINEAGE_UNARY], index);
			}
			for (idx_t i = 0; i < lineage->size(); i++) {
				shared_ptr<LineageDataWithOffset> this_data = (*lineage.get())[i].data;
				(*lineage.get())[i].source = this_data->data->Backward((*lineage.get())[i].source) + this_data->child_offset;
			}
			co_yield make_unique<LineageResVal>(LineageResVal(move(lineage)));
		}
		co_return;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_UNARY], index);
		}
		for (idx_t i = 0; i < lineage->size(); i++) {
			(*lineage.get())[i] = {
				(*lineage.get())[i].data->data->Backward((*lineage.get())[i].source),
				(*lineage.get())[i].data->data->GetChild()
			};
		}
		Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(lineage);
		while (child_gen.Next()) {
			co_yield child_gen.GetValue();
		}
		co_return;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// we need hash table from the build side
		// access the probe side, get the address from the right side
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_PROBE], index);
		}
		unique_ptr<vector<SourceAndMaybeData>> right_lineage = make_unique<vector<SourceAndMaybeData>>();
		unique_ptr<vector<SourceAndMaybeData>> left_lineage = make_unique<vector<SourceAndMaybeData>>();

		right_lineage->reserve(lineage->size());
		left_lineage->reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			// get the backward lineage for id=source
			SourceAndMaybeData source = (*lineage.get())[i];
			auto data_index = dynamic_cast<LineageNested &>(*source.data->data).LocateChunkIndex(source.source);
			auto binary_data = dynamic_cast<LineageNested &>(*source.data->data).GetChunkAt(data_index);
			idx_t adjust_offset = 0;
			if (data_index > 0) {
				// adjust the source
				adjust_offset = dynamic_cast<LineageNested &>(*source.data->data).GetAccCount(data_index-1);
			}
			if (dynamic_cast<LineageBinary&>(*binary_data->data).right != nullptr) {
				right_lineage->push_back({
					dynamic_cast<LineageBinary &>(*binary_data->data).right->Backward(source.source - adjust_offset),
					binary_data->data->GetChild()
				});
			}

			if (dynamic_cast<LineageBinary&>(*binary_data->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*binary_data->data).left->Backward(source.source - adjust_offset);
				if (left == 0) {
					continue;
				}
				if (offset == 0) {
					left_lineage->push_back({0, nullptr});
				} else {
					bool flag = false;
					for (idx_t it = 0; it < hm_range.size();  ++it) {
						if (left >= hm_range[it].first && left <= hm_range[it].second) {
							auto val = ((left - hm_range[it].first) / offset) + hash_chunk_count[it];
							left_lineage->push_back({val, nullptr}); // Full scan
							flag = true;
							break;
						}
					}
					D_ASSERT(flag);
				}
			}
		}
		auto left = children[1]->Backward(move(right_lineage));
		auto right = children[0]->Backward(move(left_lineage));
		co_yield make_unique<LineageResJoin>(LineageResJoin(move(left), move(right)));
		co_return;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_SOURCE], index);
		}

		if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
			vector<SourceAndMaybeData> orig_lineage = vector<SourceAndMaybeData>(*lineage.get());
			lineage->clear();

			for (const SourceAndMaybeData &source : orig_lineage) {
				auto payload = (uint64_t *)source.data->data->Process(0);
				auto val = payload[source.source];
				// iterate the index to find potential chunks
				auto flag = false;
				for (idx_t it = 0; it < hm_range.size(); ++it) {
					if (val >= hm_range[it].first && val <= hm_range[it].second) {
						// scan this chunk
						LineageDataWithOffset this_data = data[LINEAGE_SINK][it];
						idx_t res_count = this_data.data->Count();
						auto sink_payload = (uint64_t *)this_data.data->Process(0);
						for (idx_t it2 = 0; it2 < res_count; ++it2) {
							if (sink_payload[it2] == val) {
								lineage->push_back({it2 + hash_chunk_count[it], nullptr});
								flag = true;
								break;
							}
						}
					}
					if (flag) {
						break;
					}
				}
			}
			// TODO yield earlier?
			Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(lineage);
			while (child_gen.Next()) {
				co_yield child_gen.GetValue();
			}
		} else {
			for (const SourceAndMaybeData& source : *lineage.get()) {
				auto payload = (uint64_t*)source.data->data->Process(0);
				auto res_list = hash_map_agg[payload[source.source]];
				Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(make_shared<vector<SourceAndMaybeData>>(res_list));
				while (child_gen.Next()) {
					co_yield child_gen.GetValue();
				}
			}
		}
		co_return;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_SOURCE], index);
		}

		if (data[LINEAGE_SOURCE].size() > PROBE_SIZE) {
			vector<SourceAndMaybeData> orig_lineage = vector<SourceAndMaybeData>(*lineage.get());
			lineage->clear();

			for (const SourceAndMaybeData &source : orig_lineage) {
				auto payload = (sel_t *)source.data->data->Process(0);
				auto val = payload[source.source];
				// iterate the index to find potential chunks
				auto flag = false;
				for (idx_t it = 0; it < hm_range.size(); ++it) {
					if (val >= hm_range[it].first && val <= hm_range[it].second) {
						// scan this chunk
						LineageDataWithOffset this_data = data[LINEAGE_SINK][it];
						idx_t res_count = this_data.data->Count();
						auto sink_payload = (sel_t *)this_data.data->Process(0);
						for (idx_t it2 = 0; it2 < res_count; ++it2) {
							if (sink_payload[it2] == val) {
								lineage->push_back({it2 + hash_chunk_count[it], nullptr});
								flag = true;
								break;
							}
						}
					}
					if (flag) {
						break;
					}
				}
				// TODO yield earlier?
				Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(lineage);
				while (child_gen.Next()) {
					co_yield child_gen.GetValue();
				}
			}
		} else {
			for (const SourceAndMaybeData& source : *lineage.get()) {
				auto payload = (sel_t*)source.data->data->Process(0);
				auto res_list = hash_map_agg[payload[source.source]];
				Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(make_shared<vector<SourceAndMaybeData>>(res_list));
				while (child_gen.Next()) {
					co_yield child_gen.GetValue();
				}
			}
		}
		co_return;
	}
	case PhysicalOperatorType::PROJECTION: {
		// TODO this feels wasteful...
		Generator<unique_ptr<LineageRes>> child_gen = children[0]->Backward(move(lineage));
		while (child_gen.Next()) {
			co_yield child_gen.GetValue();
		}
		co_return;
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_PROBE], index);
		}

		shared_ptr<vector<SourceAndMaybeData>> left_lineage = make_shared<vector<SourceAndMaybeData>>();
		left_lineage->reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			SourceAndMaybeData source = (*lineage.get())[i];

			if (dynamic_cast<LineageBinary&>(*source.data->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*source.data->data).left->Backward(source.source);
				left_lineage->push_back({left, source.data->data->GetChild()});
			}

			if (dynamic_cast<LineageBinary&>(*source.data->data).right != nullptr) {
				auto right = dynamic_cast<LineageBinary&>(*source.data->data).right->Backward(source.source);
				(*lineage.get())[i] = {right, nullptr}; // Full scan
			}
		}
		auto left = children[1]->Backward(move(lineage));
		auto right = children[0]->Backward(move(left_lineage));
		co_yield make_unique<LineageResJoin>(LineageResJoin(move(left), move(right)));
		co_return;
	} case PhysicalOperatorType::CROSS_PRODUCT: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_PROBE], index);
		}

		shared_ptr<vector<SourceAndMaybeData>> left_lineage = make_shared<vector<SourceAndMaybeData>>();
		left_lineage->reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			left_lineage->push_back({(*lineage.get())[i].source, (*lineage.get())[i].data->data->GetChild()});
			(*lineage.get())[i] = {(*lineage.get())[i].data->data->Backward((*lineage.get())[i].source), nullptr}; // Full scan
		}
		auto left = children[1]->Backward(move(lineage));
		auto right = children[0]->Backward(move(left_lineage));
		co_yield make_unique<LineageResJoin>(LineageResJoin(move(left), move(right)));
		co_return;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_UNARY], index);
		}

		vector<SourceAndMaybeData> left_lineage;
		left_lineage.reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			SourceAndMaybeData source = (*lineage.get())[i];

			if (dynamic_cast<LineageBinary&>(*source.data->data).right != nullptr) {
				// This is the exact value - no need to iterate
				auto right = dynamic_cast<LineageBinary&>(*source.data->data).right->Backward(source.source);
				left_lineage.push_back({right, nullptr});
			}

			if (dynamic_cast<LineageBinary&>(*source.data->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*source.data->data).left->Backward(source.source);
				(*lineage.get())[i] = {left, source.data->data->GetChild()};
			}
		}
		auto left = GenWrapper(make_unique<LineageResVal>(LineageResVal(make_unique<vector<SourceAndMaybeData>>(left_lineage))));
		auto right = children[0]->Backward(move(lineage));
		co_yield make_unique<LineageResJoin>(LineageResJoin(move(left), move(right)));
		co_return;
	}
	case PhysicalOperatorType::ORDER_BY: {
		if ((*lineage.get())[0].data == nullptr) {
			// No OrderBy index since it's all one chunk - just get that chunk
			auto data_ptr = make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY][0]);
			for (idx_t i = 0; i < lineage->size(); i++) {
				(*lineage.get())[i].data = data_ptr;
			}
		}

		for (idx_t i = 0; i < lineage->size(); i++) {
			(*lineage.get())[i] = {
				(*lineage.get())[i].data->data->Backward((*lineage.get())[i].source),
				nullptr // requires full scan
			};
		}
		auto child_gen = children[0]->Backward(move(lineage));
		while (child_gen.Next()) {
			co_yield child_gen.GetValue();
		}
		co_return;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Recurse until we find a filter-like child, then use all of its lineage
		// This optimizations allows us to skip aggregations and order bys - especially helping for query 15
		SimpleAggQueryStruct agg_struct = RecurseForSimpleAgg(children[0]);

		if (agg_struct.child_lineage_data_vector.empty()) {
			co_yield make_unique<LineageResVal>(LineageResVal({}));
			co_return;
		}

		for (const LineageDataWithOffset& child_lineage_data : agg_struct.child_lineage_data_vector) {
			shared_ptr<vector<SourceAndMaybeData>> child_lineage = make_shared<vector<SourceAndMaybeData>>();
			child_lineage->reserve(child_lineage_data.data->Count());
			for (idx_t i = 0; i < child_lineage_data.data->Count(); i++) {
				child_lineage->push_back({i, make_unique<LineageDataWithOffset>(child_lineage_data)});
			}
			Generator<unique_ptr<LineageRes>> child_gen = agg_struct.materialized_child_op->Backward(move(child_lineage));
			while (child_gen.Next()) {
				co_yield child_gen.GetValue();
			}
		}
		co_return;
	}
	default: {
		// We must capture lineage for everything that BACKWARD is called on
		D_ASSERT(false);
	}
	}
}

vector<idx_t> LineageManager::Backward(PhysicalOperator *op, idx_t source) {
	// an operator can have lineage from multiple threads, how to decide which one to check?
	shared_ptr<vector<SourceAndMaybeData>> sources = make_shared<vector<SourceAndMaybeData>>();
	sources->push_back({source, nullptr});
	Generator<unique_ptr<LineageRes>> lineage_gen = op->lineage_op.at(-1)->Backward(move(sources));
	vector<idx_t> res;
	while (lineage_gen.Next()) {
		vector<idx_t> partial_res = lineage_gen.GetValue()->GetValues();
		res.insert(res.end(), partial_res.begin(), partial_res.end());
	}
	return res;
}

idx_t LineageManager::BackwardCount(PhysicalOperator *op, idx_t source) {
	// an operator can have lineage from multiple threads, how to decide which one to check?
	shared_ptr<vector<SourceAndMaybeData>> sources = make_shared<vector<SourceAndMaybeData>>();
	sources->push_back({source, nullptr});
	Generator<unique_ptr<LineageRes>> lineage_gen = op->lineage_op.at(-1)->Backward(move(sources));
	idx_t count = 0;
	while (lineage_gen.Next()) {
		count += lineage_gen.GetValue()->GetCount();
	}
	return count;
}


// LineageResJoin

vector<idx_t> LineageResJoin::GetValues() {
	// Need to block on each side to preserve prov polynomial semantics
	vector<idx_t> left;
	while (left_gen.Next()) {
		vector<idx_t> left_vec = left_gen.GetValue()->GetValues();
		left.insert(left.end(), left_vec.begin(), left_vec.end());
	}
	vector<idx_t> right;
	while (right_gen.Next()) {
		vector<idx_t> right_vec = right_gen.GetValue()->GetValues();
		right.insert(right.end(), right_vec.begin(), right_vec.end());
	}
	vector<idx_t> res;
	res.reserve(left.size() + right.size());
	res.insert(res.end(), left.begin(), left.end());
	res.insert(res.end(), right.begin(), right.end());
	return res;
}

idx_t LineageResJoin::GetCount() {
	// Since we're just getting count, no need to block!
	idx_t count = 0;
	while (left_gen.Next()) {
		count += left_gen.GetValue()->GetCount();
	}
	while (right_gen.Next()) {
		count += right_gen.GetValue()->GetCount();
	}
	return count;
}


// LineageResVal

vector<idx_t> LineageResVal::GetValues() {
	if (vals == nullptr) {
		return {};
	}
	vector<idx_t> res;
	res.reserve(vals->size());
	for (idx_t i = 0; i < vals->size(); i++) {
		res.push_back((*vals.get())[i].source);
	}
	return res;
}

idx_t LineageResVal::GetCount() {
	return vals == nullptr ? 0 : vals->size();
}

} // namespace duckdb
#endif
