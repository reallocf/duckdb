#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;

void LineageManager::PostProcess(PhysicalOperator *op, bool should_index) {
	// massage the data to make it easier to query
	bool always_post_process =
	    op->type == PhysicalOperatorType::HASH_GROUP_BY
	    || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
	    || op->type == PhysicalOperatorType::HASH_JOIN;
	bool never_post_process =
	    op->type == PhysicalOperatorType::ORDER_BY; // 1 large chunk, so index is useless
	if ((always_post_process || should_index) && !never_post_process) {
		vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
		for (idx_t i = 0; i < table_column_types.size(); i++) {
			bool skip_this_sel_vec =
				(op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_COMBINE)
			    || (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_COMBINE)
			    || (op->type == PhysicalOperatorType::HASH_GROUP_BY && i == LINEAGE_SOURCE && !should_index)
			    || (op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY && i == LINEAGE_SOURCE && !should_index)
			    || (op->type == PhysicalOperatorType::HASH_JOIN && i == LINEAGE_PROBE && !should_index
			        && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::MARK);
			if (skip_this_sel_vec) {
				continue;
			}
			// for hash join, build hash table on the build side that map the address to id
			// for group by, build hash table on the unique groups
			for (auto const& lineage_op : op->lineage_op) {
				LineageProcessStruct lps = lineage_op.second->PostProcess(0, 0, lineage_op.first);
				while (lps.still_processing) {
					lps = lineage_op.second->PostProcess(lps.count_so_far,  lps.size_so_far, lineage_op.first);
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


LineageProcessStruct OperatorLineage::PostProcess(idx_t count_so_far, idx_t size_so_far, int thread_id) {
	if (data[finished_idx].size() > data_idx) {
		Vector thread_id_vec(Value::INTEGER(thread_id));
		switch (this->type) {
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();
			index.push_back(res_count + count_so_far);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			// Hash Join - other joins too?
			if (finished_idx == LINEAGE_BUILD) {
				// build hash table
				LineageDataWithOffset this_data = data[LINEAGE_BUILD][data_idx];
				auto payload = (uint64_t*)this_data.data->Process(0);
				idx_t res_count = this_data.data->Count();
				for (idx_t i=0; i < res_count; ++i) {
					hash_map[payload[i]] = {i + count_so_far, nullptr};
				}
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else {
				idx_t res_count = data[LINEAGE_PROBE][data_idx].data->Count();
				index.push_back(res_count + count_so_far);

				count_so_far += res_count;
				size_so_far +=  data[LINEAGE_PROBE][data_idx].data->Size();
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
				if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
					auto payload = (sel_t*)this_data.data->Process(0);
					for (idx_t i=0; i < res_count; ++i) {
						hash_map_agg[(idx_t)payload[i]].push_back({i + count_so_far, nullptr});
					}
				} else {
					auto payload = (uint64_t*)this_data.data->Process(0);
					for (idx_t i=0; i < res_count; ++i) {
						hash_map_agg[(idx_t)payload[i]].push_back({i + count_so_far, nullptr});
					}
				}
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else if (finished_idx == LINEAGE_COMBINE) {
			} else {
				idx_t res_count = data[LINEAGE_SOURCE][data_idx].data->Count();
				index.push_back(res_count + count_so_far);
				count_so_far += res_count;
				size_so_far +=  data[LINEAGE_SOURCE][data_idx].data->Size();
			}
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();
			index.push_back(res_count + count_so_far);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();
			index.push_back(res_count + count_so_far);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::INDEX_JOIN: {
			LineageDataWithOffset this_data = data[0][data_idx];
			idx_t res_count = this_data.data->Count();
			index.push_back(res_count + count_so_far);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
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
	return LineageProcessStruct{ count_so_far, size_so_far, data[finished_idx].size() > data_idx };
}

void AccessLineageDataViaIndex(
    const shared_ptr<vector<SourceAndMaybeData>>& lineage, // We believe that there is no data in any of these if it's passed here
    const vector<LineageDataWithOffset>& data,
    const vector<idx_t>& index
) {
	for (idx_t i = 0; i < lineage->size(); i++) {
		SourceAndMaybeData source = (*lineage.get())[i];
		// we need a way to locate the exact data we should access
		// from the source index
		auto lower = lower_bound(index.begin(), index.end(), source.source);
		if (lower == index.end()) {
			throw std::logic_error("Out of bounds lineage requested");
		}
		auto chunk_id = std::distance(index.begin(), lower);
		if (*lower == source.source) {
			chunk_id += 1;
		}
		auto this_data = data[chunk_id];
		if (chunk_id > 0) {
			(*lineage.get())[i].source -= index[chunk_id-1];
		}
		(*lineage.get())[i].data = make_shared<LineageDataWithOffset>(this_data);
	}
}

void OperatorLineage::Backward(const shared_ptr<vector<SourceAndMaybeData>>& lineage) {
	if (lineage->empty()) {
		// Skip if empty
		return;
	}
	switch (this->type) {
	case (PhysicalOperatorType::DELIM_JOIN): {
		// distinct input is delim join input
		// distinct should be the input to delim scan
		children[2]->children.push_back(children[0]);

		// chunk scan input is delim join input
		children[1]->children[1] = children[0];
		children[1]->Backward(lineage);
		break;
	}
	case PhysicalOperatorType::DELIM_SCAN: {
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		// End of the recursion!
		if (data[LINEAGE_UNARY].empty() && (*lineage.get())[0].data == nullptr) {
			// Nothing to do! Lineage correct as-is
		} else {
			if ((*lineage.get())[0].data == nullptr) {
				AccessLineageDataViaIndex(lineage, data[LINEAGE_UNARY], index);
			}
			for (idx_t i = 0; i < lineage->size(); i++) {
				shared_ptr<LineageDataWithOffset> this_data = (*lineage.get())[i].data;
				(*lineage.get())[i].source = this_data->data->Backward((*lineage.get())[i].source) + this_data->offset;
			}
		}
		break;
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
		children[0]->Backward(lineage);
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// we need hash table from the build side
		// access the probe side, get the address from the right side
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_PROBE], index);
		}
		shared_ptr<vector<SourceAndMaybeData>> right_lineage = make_shared<vector<SourceAndMaybeData>>();
		right_lineage->reserve(lineage->size());
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
				(*lineage.get())[i] = hash_map[left]; // Full scan
			}
		}
		children[0]->Backward(lineage);
		children[1]->Backward(right_lineage);
		lineage->reserve(lineage->size() + right_lineage->size());
		lineage->insert(lineage->begin(), right_lineage->begin(), right_lineage->end());
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_SOURCE], index);
		}

		vector<SourceAndMaybeData> orig_lineage = vector<SourceAndMaybeData>(*lineage.get());
		lineage->clear();
		// First find size to reserve
		idx_t lineage_size = 0;
		for (const SourceAndMaybeData& source : orig_lineage) {
			auto payload = (uint64_t*)source.data->data->Process(0);
			lineage_size += hash_map_agg[payload[source.source]].size();
		}
		lineage->reserve(lineage_size);
		// Now fill TODO is it right to do two passes like this? Yes, this genuinely is faster :)
		for (const SourceAndMaybeData& source : orig_lineage) {
			auto payload = (uint64_t*)source.data->data->Process(0);
			auto res_list = hash_map_agg[payload[source.source]];
			lineage->insert(lineage->end(), res_list.begin(), res_list.end());
		}
		children[0]->Backward(lineage);
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_SOURCE], index);
		}

		vector<SourceAndMaybeData> orig_lineage = vector<SourceAndMaybeData>(*lineage.get());
		lineage->clear();
		// First find size to reserve
		idx_t lineage_size = 0;
		for (const SourceAndMaybeData& source : orig_lineage) {
			auto payload = (sel_t*)source.data->data->Process(0);
			lineage_size += hash_map_agg[payload[source.source]].size();
		}
		lineage->reserve(lineage_size);
		// Now fill TODO is it right to do two passes like this? Yes, this genuinely is faster :)
		for (const SourceAndMaybeData& source : orig_lineage) {
			auto payload = (sel_t*)source.data->data->Process(0);
			auto res_list = hash_map_agg[payload[source.source]];
			lineage->insert(lineage->end(), res_list.begin(), res_list.end());
		}
		children[0]->Backward(lineage);
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		children[0]->Backward(lineage);
		break;
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
		children[1]->Backward(lineage);
		children[0]->Backward(left_lineage);
		lineage->reserve(lineage->size() + left_lineage->size());
		lineage->insert(lineage->end(), left_lineage->begin(), left_lineage->end());
		break;
	} case PhysicalOperatorType::CROSS_PRODUCT: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_PROBE], index);
		}

		shared_ptr<vector<SourceAndMaybeData>> left_lineage = make_shared<vector<SourceAndMaybeData>>();
		left_lineage->reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			SourceAndMaybeData source = (*lineage.get())[i];

			left_lineage->push_back({source.source, source.data->data->GetChild()});
			(*lineage.get())[i] = {source.data->data->Backward(source.source), nullptr}; // Full scan
		}
		children[1]->Backward(lineage);
		children[0]->Backward(left_lineage);
		lineage->reserve(lineage->size() + left_lineage->size());
		lineage->insert(lineage->end(), left_lineage->begin(), left_lineage->end());
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		if ((*lineage.get())[0].data == nullptr) {
			AccessLineageDataViaIndex(lineage, data[LINEAGE_UNARY], index);
		}

		shared_ptr<vector<SourceAndMaybeData>> new_lineage = make_shared<vector<SourceAndMaybeData>>();
		new_lineage->reserve(lineage->size());
		for (idx_t i = 0; i < lineage->size(); i++) {
			SourceAndMaybeData source = (*lineage.get())[i];

			if (dynamic_cast<LineageBinary&>(*source.data->data).right != nullptr) {
				// This is the exact value - no need to iterate
				auto right = dynamic_cast<LineageBinary&>(*source.data->data).right->Backward(source.source);
				(*lineage.get())[i] = {right, nullptr};
			}

			if (dynamic_cast<LineageBinary&>(*source.data->data).left != nullptr) {
				auto left = dynamic_cast<LineageBinary&>(*source.data->data).left->Backward(source.source);
				new_lineage->push_back({left, source.data->data->GetChild()});
			}
		}
		children[0]->Backward(new_lineage);
		lineage->reserve(lineage->size() + new_lineage->size());
		lineage->insert(lineage->end(), new_lineage->begin(), new_lineage->end());
		break;
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
		children[0]->Backward(lineage);
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Every child index is part of lineage - potential lineage explosion incoming
		auto child = children[0];
		vector<LineageDataWithOffset> child_lineage_data_vector;
		switch (child->type) {
		case PhysicalOperatorType::TABLE_SCAN:
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::ORDER_BY:
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
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			child_lineage_data_vector = child->data[LINEAGE_SINK];
			break;
		}
		default: {
			throw std::logic_error("We must capture lineage for everything that BACKWARD is called on");
		}
		}

		lineage->clear(); // We don't care about lineage passed here since simple agg always yields EVERY child value
		if (child_lineage_data_vector.empty()) {
			return;
		}
		LineageDataWithOffset last = child_lineage_data_vector[child_lineage_data_vector.size() - 1];
		idx_t output_size = last.offset + last.data->Count();
		lineage->reserve(output_size);
		for (const LineageDataWithOffset& child_lineage_data : child_lineage_data_vector) {
			shared_ptr<vector<SourceAndMaybeData>> child_lineage = make_shared<vector<SourceAndMaybeData>>();
			child_lineage->reserve(child_lineage_data.data->Count());
			auto child_data_ptr = make_shared<LineageDataWithOffset>(child_lineage_data);
			for (idx_t i = 0; i < child_lineage_data.data->Count(); i++) {
				child_lineage->push_back({i, child_data_ptr});
			}
			child->Backward(child_lineage);
			lineage->insert(lineage->end(), child_lineage->begin(), child_lineage->end());
		}
		break;
	}
	default: {
		// We must capture lineage for everything that BACKWARD is called on
		D_ASSERT(false);
	}
	}
}

vector<SourceAndMaybeData> LineageManager::Backward(PhysicalOperator *op, idx_t source) {
	// an operator can have lineage from multiple threads, how to decide which one to check?
	shared_ptr<vector<SourceAndMaybeData>> lineage = make_shared<vector<SourceAndMaybeData>>();
	lineage->push_back({source, nullptr});
	op->lineage_op.at(-1)->Backward(lineage);
	return *lineage.get();
}

} // namespace duckdb
#endif
