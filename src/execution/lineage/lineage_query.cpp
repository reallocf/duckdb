#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;

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
			    || (op->type == PhysicalOperatorType::HASH_JOIN && i == LINEAGE_PROBE && !should_index);
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
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), false);
		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), false);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i], false);
		}
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		bool child_should_index =
			op->type == PhysicalOperatorType::HASH_GROUP_BY
			|| op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
			|| (op->type == PhysicalOperatorType::HASH_JOIN && i == 0) // Only build side child needs an index
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
					hash_map[payload[i]] = i + count_so_far;
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
						hash_map_agg[(idx_t)payload[i]].push_back(i + count_so_far);
					}
				} else {
					auto payload = (uint64_t*)this_data.data->Process(0);
					for (idx_t i=0; i < res_count; ++i) {
						hash_map_agg[(idx_t)payload[i]].push_back(i + count_so_far);
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

struct BackwardHelper {
	LineageDataWithOffset data;
	idx_t source;
};

BackwardHelper AccessLineageDataViaIndex(idx_t source, vector<LineageDataWithOffset> data, vector<idx_t> index) {
	// we need a way to locate the exact data we should access
	// from the source index
	auto lower = lower_bound(index.begin(), index.end(), source);
	if (lower == index.end()) {
		throw std::logic_error("Out of bounds lineage requested");
	}
	auto chunk_id = std::distance(index.begin(), lower);
	if (*lower == source) {
		chunk_id += 1;
	}
	auto this_data = data[chunk_id];
	if (chunk_id > 0) {
		source -= index[chunk_id-1];
	}
	return {this_data, source};
}

vector<idx_t> OperatorLineage::Backward(PhysicalOperator *op, idx_t source, shared_ptr<LineageDataWithOffset> maybe_lineage_data) {
	LineageDataWithOffset this_data;
	switch (this->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		// End of the recursion!
		if (data[LINEAGE_UNARY].empty()) {
			// Handle case where no lineage captured for the TABLE_SCAN
			if (maybe_lineage_data == nullptr) {
				return {source};
			} else {
				return {source + maybe_lineage_data->offset};
			}
		}
		if (maybe_lineage_data == nullptr) {
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_UNARY], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}
		auto res = this_data.data->Backward(source);
		return {res + this_data.offset};
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT: {
		if (maybe_lineage_data == nullptr) {
			// we need a way to locate the exact data we should access
			// from the source index
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_UNARY], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}
		auto res = this_data.data->Backward(source);
		auto child_lop = op->children[0]->lineage_op.at(-1);
		return child_lop->Backward(op->children[0].get(), res, this_data.data->GetChild());
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// we need hash table from the build side
		// access the probe side, get the address from the right side
		if (maybe_lineage_data == nullptr) {
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_PROBE], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}
		vector<idx_t> lineage;

		// get the backward lineage for id=source
		auto data_index = dynamic_cast<LineageNested &>(*this_data.data).LocateChunkIndex(source);
		auto binary_data = dynamic_cast<LineageNested &>(*this_data.data).GetChunkAt(data_index);
		idx_t adjust_offset = 0;
		if (data_index > 0) {
			// adjust the source
			adjust_offset = dynamic_cast<LineageNested &>(*this_data.data).GetAccCount(data_index-1);
		}
		if (dynamic_cast<LineageBinary&>(*binary_data->data).right != nullptr) {
			auto right = dynamic_cast<LineageBinary&>(*binary_data->data).right->Backward(source - adjust_offset);
			auto child_lop = op->children[1]->lineage_op.at(-1);
			vector<idx_t> right_lineage = child_lop->Backward(op->children[1].get(), right, binary_data->data->GetChild());
			lineage.reserve(lineage.size() + right_lineage.size());
			lineage.insert(lineage.end(), right_lineage.begin(), right_lineage.end());
		}

		if (dynamic_cast<LineageBinary&>(*binary_data->data).left != nullptr) {
			auto left = dynamic_cast<LineageBinary&>(*binary_data->data).left->Backward(source - adjust_offset);
			left = hash_map[left];
			auto child_lop = op->children[0]->lineage_op.at(-1);
			vector<idx_t> left_lineage = child_lop->Backward(op->children[0].get(), left); // requires full scan
			lineage.reserve(lineage.size() + left_lineage.size());
			lineage.insert(lineage.end(), left_lineage.begin(), left_lineage.end());
		}

		return lineage;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		vector<idx_t> lineage;
		if (maybe_lineage_data == nullptr) {
			// get the address it maps to from probe, then use it to access all the groups
			// that maps to this
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_SOURCE], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}

		auto payload = (uint64_t*)this_data.data->Process(0);
		auto child_lop = op->children[0]->lineage_op.at(-1);
		for (auto val : hash_map_agg[payload[source]]) {
			vector<idx_t> elem_lineage = child_lop->Backward(op->children[0].get(), val); // requires full scan
			lineage.reserve(lineage.size() + elem_lineage.size());
			lineage.insert(lineage.end(), elem_lineage.begin(), elem_lineage.end());
		}

		return lineage;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		vector<idx_t> lineage;
		if (maybe_lineage_data == nullptr) {
			// get the address it maps to from probe, then use it to access all the groups
			// that maps to this
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_SOURCE], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}

		auto payload = (sel_t *)this_data.data->Process(0);
		auto child_lop = op->children[0]->lineage_op.at(-1);
		for (auto val : hash_map_agg[payload[source]]) {
			vector<idx_t> elem_lineage = child_lop->Backward(op->children[0].get(), val); // requires full scan
			lineage.reserve(lineage.size() + elem_lineage.size());
			lineage.insert(lineage.end(), elem_lineage.begin(), elem_lineage.end());
		}

		return lineage;
	}
	case PhysicalOperatorType::PROJECTION: {
		auto child_lop = op->children[0]->lineage_op.at(-1);
		return child_lop->Backward(op->children[0].get(), source, maybe_lineage_data);
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		vector<idx_t> lineage;
		if (maybe_lineage_data == nullptr) {
			// get the address it maps to from probe, then use it to access all the groups
			// that maps to this
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_PROBE], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}

		// TODO iteration on these
		if (dynamic_cast<LineageBinary&>(*this_data.data).right != nullptr) {
			auto right = dynamic_cast<LineageBinary&>(*this_data.data).right->Backward(source);
			lineage.push_back(right);
		}

		if (dynamic_cast<LineageBinary&>(*this_data.data).left != nullptr) {
			auto left = dynamic_cast<LineageBinary&>(*this_data.data).left->Backward(source);
			lineage.push_back(left+this_data.offset);
		}

		return lineage;
	} case PhysicalOperatorType::CROSS_PRODUCT: {
		vector<idx_t> lineage;
		if (maybe_lineage_data == nullptr) {
			// get the address it maps to from probe, then use it to access all the groups
			// that maps to this
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_PROBE], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}

		// TODO iteration on these
		lineage.push_back(this_data.data->Backward(source));
		lineage.push_back(this_data.offset + source);

		return lineage;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		vector<idx_t> lineage;
		if (maybe_lineage_data == nullptr) {
			// get the address it maps to from probe, then use it to access all the groups
			// that maps to this
			auto bh = AccessLineageDataViaIndex(source, data[LINEAGE_UNARY], index);
			this_data = bh.data;
			source = bh.source;
		} else {
			this_data = *maybe_lineage_data.get();
		}

		// TODO iteration on these
		if (dynamic_cast<LineageBinary&>(*this_data.data).right != nullptr) {
			auto right = dynamic_cast<LineageBinary&>(*this_data.data).right->Backward(source);
			lineage.push_back(right);
		}

		if (dynamic_cast<LineageBinary&>(*this_data.data).left != nullptr) {
			auto left = dynamic_cast<LineageBinary&>(*this_data.data).left->Backward(source);
			lineage.push_back(left+this_data.offset);
		}

		return lineage;
	}
	case PhysicalOperatorType::ORDER_BY: {
		if (maybe_lineage_data == nullptr) {
			// No OrderBy index since it's all one chunk - just get that chunk
			this_data = data[LINEAGE_UNARY][0];
		} else {
			this_data = *maybe_lineage_data.get();
		}

		auto res = this_data.data->Backward(source);
		auto child_lop = op->children[0]->lineage_op.at(-1);
		return child_lop->Backward(op->children[0].get(), res); // requires full scan
	}
	default: {
		// We must capture lineage for everything that BACKWARD is called on
		D_ASSERT(false);
		return {};
	}
	}
}

vector<idx_t> LineageManager::Backward(PhysicalOperator *op, idx_t source) {
	// an operator can have lineage from multiple threads, how to decide which one to check?
	vector<idx_t> lineage = op->lineage_op.at(-1)->Backward(op, source);
	return lineage;
}

} // namespace duckdb
#endif
