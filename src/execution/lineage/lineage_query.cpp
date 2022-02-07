#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;

void LineageManager::PostProcess(PhysicalOperator *op) {
	// massage the data to make it easier to query
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
	for (idx_t i = 0; i < table_column_types.size(); i++) {
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


	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		PostProcess( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get());
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i)
			PostProcess( dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		PostProcess(op->children[i].get());
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
				auto payload = this_data.data->Process(0);
				for (idx_t i=0; i < res_count; ++i) {
					hash_map[payload[i]] = i + count_so_far;
				}
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else if (finished_idx == LINEAGE_COMBINE) {
			} else {
			}
			break;
		}
		default:
			// We must capture lineage for everything getting processed
			D_ASSERT(false);
		}
	}
	data_idx++;
	return LineageProcessStruct{ count_so_far, size_so_far, data[finished_idx].size() > data_idx };
}

vector<idx_t> OperatorLineage::Backward(PhysicalOperator *op, idx_t source) {
	vector<idx_t> lineage;
	switch (this->type) {
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::TABLE_SCAN: {
		// we need a way to locate the exact data we should access
		// from the source index
		auto lower = lower_bound(index.begin(), index.end(), source);
		if (lower == index.end()) {
			return lineage;
		}
		auto chunk_id =  std::distance(index.begin(), lower);
		LineageDataWithOffset this_data = data[LINEAGE_UNARY][chunk_id];
		if (chunk_id > 0) {
			source -= index[chunk_id-1];
		}
		auto res = this_data.data->Backward(source);
		if (!op->children.empty()) {
			lineage = op->children[0]->lineage_op.at(-1)->Backward(op->children[0].get(), res+this_data.offset);
		}
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// we need hash table from the build side
		// access the probe side, get the address from the right side
		auto lower = lower_bound(index.begin(), index.end(), source);
		if (lower == index.end()) {
			std::cout << " source (" << source <<") not found in index" << std::endl;
			return lineage;
		}
		auto chunk_id =  std::distance(index.begin(), lower);
		if (*lower == source) {
			chunk_id += 1;
		}
		LineageDataWithOffset this_data = data[LINEAGE_PROBE][chunk_id];
		if (chunk_id > 0) source -= index[chunk_id-1];

		// get the backward lineage for id=source
		auto data_index = dynamic_cast<LineageNested &>(*this_data.data).LocateChunkIndex(source);
		auto BinaryData = dynamic_cast<LineageNested &>(*this_data.data).GetChunkAt(data_index);
		idx_t adjust_offset = 0;
		if (data_index > 0) {
			// adjust the source
			adjust_offset = dynamic_cast<LineageNested &>(*this_data.data).GetAccCount(data_index-1);
		}
		if (dynamic_cast<LineageBinary&>(*BinaryData->data).right != nullptr) {
			auto left = dynamic_cast<LineageBinary&>(*BinaryData->data).right->Backward(source - adjust_offset);
			lineage.push_back(left+BinaryData->offset);
		}

		if (dynamic_cast<LineageBinary&>(*BinaryData->data).left != nullptr) {
			auto right = dynamic_cast<LineageBinary&>(*BinaryData->data).left->Backward(source - adjust_offset);
			lineage.push_back(hash_map[right]);
		}
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		lineage = op->children[0]->lineage_op.at(-1)->Backward(op->children[0].get(), source);
		break;
	}
	default: {}
	}
	return lineage;
}

vector<idx_t> LineageManager::Backward(PhysicalOperator *op, idx_t source) {
	// an operator can have lineage from multiple threads, how to decide which one to check?
	vector<idx_t> lineage = op->lineage_op.at(-1)->Backward(op, source);
	return lineage;
}

} // namespace duckdb
#endif
