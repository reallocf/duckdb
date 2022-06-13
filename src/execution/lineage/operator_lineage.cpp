#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id) {
	if (!trace_lineage || datum->Count() == 0) return;
	// Prepare this vector's chunk to be passed on to future operators
	pipeline_lineage->AdjustChunkOffsets(datum->Count(), lineage_idx);

	// Set child ptr
	datum->SetChild(GetChildLatest(lineage_idx));

	// Capture this vector
	idx_t child_offset = pipeline_lineage->GetChildChunkOffset(lineage_idx);
	idx_t this_offset = GetThisOffset(lineage_idx);
	if (lineage_idx == LINEAGE_COMBINE) {
		data[lineage_idx].push_back(LineageDataWithOffset{datum, thread_id, this_offset});
	} else {
		data[lineage_idx].push_back(LineageDataWithOffset{datum, (int)child_offset, this_offset});
	}

	// Add to index
	if (should_index) {
		switch (type) {
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::INDEX_JOIN:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			// Binary Search index
			idx_t index_last = index.empty() ? 0 : index[index.size() - 1];
			index.push_back(index_last + datum->Count());
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			if (lineage_idx == LINEAGE_PROBE) {
				// Binary Search index
				idx_t index_last = index.empty() ? 0 : index[index.size() - 1];
				index.push_back(index_last + datum->Count());
			}
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			if (lineage_idx == LINEAGE_SOURCE) {
				// Binary Search index
				idx_t index_last = index.empty() ? 0 : index[index.size() - 1];
				index.push_back(index_last + datum->Count());
			}
			break;
		}
		default:
			// We must capture lineage for everything getting indexed
			D_ASSERT(false);
		}
	}

	// Build Hash Join BUILD index
	if (type == PhysicalOperatorType::HASH_JOIN && lineage_idx == LINEAGE_BUILD) {
		// build hash table with range -> acc
		// if x in range -> then use range.start and adjust the value using acc
		LineageDataWithOffset this_data = data[LINEAGE_BUILD][data[LINEAGE_BUILD].size() - 1];
		auto payload = (uint64_t*)this_data.data->Process(0);
		idx_t count_so_far = this_data.this_offset;
		idx_t res_count = this_data.data->Count();
		if (offset == 0 && res_count > 1) {
			offset = payload[1] - payload[0];
		}

		// init base
		if (start_base == 0) {
			start_base = payload[0];
			last_base = payload[res_count - 1];
			hm_range.emplace_back(start_base, last_base);
			hash_chunk_count.push_back(0);
		} else {
			if (offset == 0) {
				offset = payload[res_count - 1] - start_base;
			}
			auto diff = (payload[res_count - 1] - start_base) / offset;
			if (diff + 1 !=  count_so_far + res_count - hash_chunk_count.back()) {
				// update the range and log the old one
				// range -> count
				// if value fall in this range, then remove the start / offset
				for (idx_t j = 0; j < res_count; ++j) {
					auto f = ((payload[j] - start_base) / offset);
					auto s = count_so_far + j - hash_chunk_count.back();
					if ( f !=  s) {
						if (j > 1) {
							hm_range.back().second = payload[j - 1]; // the previous one
						}
						hash_chunk_count.push_back(count_so_far + j);
						start_base = payload[j];
						last_base = payload[res_count - 1];
						hm_range.emplace_back(start_base, last_base);
						break;
					}
				}
			} else {
				hm_range.back().second = payload[res_count - 1];
			}
		}
	}
}

void OperatorLineage::FinishedProcessing(idx_t data_idx, idx_t finished_idx) {
	finished_idx++;
	data_idx = 0;
}

shared_ptr<PipelineLineage> OperatorLineage::GetPipelineLineage() {
	return pipeline_lineage;
}

void OperatorLineage::MarkChunkReturned() {
	dynamic_cast<PipelineJoinLineage *>(pipeline_lineage.get())->MarkChunkReturned();
}

LineageProcessStruct OperatorLineage::Process(const vector<column_t> column_ids, const vector<LogicalType>& types, idx_t count_so_far,
                                              DataChunk &insert_chunk, idx_t size_so_far, int thread_id, idx_t data_idx, idx_t finished_idx) {
	if (data[finished_idx].size() > data_idx) {
		Vector thread_id_vec(Value::INTEGER(thread_id));
		switch (this->type) {
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			// Index Join
			// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();


			Vector lhs_payload(types[1], this_data.data->Process(this_data.child_offset));
			// sink side, offset is adjusted during capture
			Vector rhs_payload(types[0], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			if(std::find(column_ids.begin(), column_ids.end(), 1) != column_ids.end())
				insert_chunk.data[1].Reference(lhs_payload);
			if(std::find(column_ids.begin(), column_ids.end(), 0) != column_ids.end())
				insert_chunk.data[0].Reference(rhs_payload);
			if(std::find(column_ids.begin(), column_ids.end(), 2) != column_ids.end())
				insert_chunk.data[2].Sequence(count_so_far, 1);
			if(std::find(column_ids.begin(), column_ids.end(), 3) != column_ids.end())
				insert_chunk.data[3].Reference(thread_id_vec);

			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::INDEX_JOIN: {
			// Index Join
			// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[0][data_idx];
			idx_t res_count = this_data.data->Count();

			Vector lhs_payload(types[0], this_data.data->Process(0)); // TODO is this right?
			Vector rhs_payload(types[1], this_data.data->Process(this_data.child_offset));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Sequence(count_so_far, 1);
			insert_chunk.data[1].Reference(lhs_payload);
			insert_chunk.data[2].Reference(rhs_payload);
			insert_chunk.data[3].Sequence(count_so_far, 1);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();
			// constant value
			Vector rhs_payload(Value::Value::INTEGER(((int*)this_data.data->Process(0))[0]));

			insert_chunk.SetCardinality(res_count);
			idx_t idx = 0;
			while(idx < res_count){
				if (column_ids[idx] == 0){
					insert_chunk.data[idx].Sequence(this_data.child_offset, 1);
				}
				else if(column_ids[idx] == 1){
					insert_chunk.data[idx].Reference(rhs_payload);
				}
				else if(column_ids[idx] == 2){
					insert_chunk.data[idx].Sequence(count_so_far, 1);
				}
				else if(column_ids[idx] == 3){
					insert_chunk.data[idx].Reference(thread_id_vec);
				}
				++idx;
			}

			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();

			// constant value
			Vector lhs_payload(Value::Value::INTEGER(this_data.child_offset +((int*)this_data.data->Process(0))[0]));
			Vector rhs_payload(types[1], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(lhs_payload);
			insert_chunk.data[1].Reference(rhs_payload);
			insert_chunk.data[2].Sequence(count_so_far, 1);
			insert_chunk.data[3].Reference(thread_id_vec);
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			// Seq Scan, Filter, Limit, etc...
			// schema: [INTEGER in_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();

			Vector in_index(types[1], this_data.data->Process(this_data.child_offset));

			insert_chunk.Reset();
			insert_chunk.SetCardinality(res_count);
			idx_t idx = 0;
			while(idx < column_ids.size()){
				if (column_ids[idx] == 0){
					insert_chunk.data[idx].Sequence(count_so_far, 1);
				}
				else if(column_ids[idx] == 1){
					insert_chunk.data[idx].Reference(in_index);
				}
				else if(column_ids[idx] == 2){
					insert_chunk.data[idx].Sequence(count_so_far, 1);
				}
				else if(column_ids[idx] == 3){
					insert_chunk.data[idx].Reference(thread_id_vec);
				}
				++idx;
			}
			count_so_far += res_count;
 			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			// Hash Join - other joins too?
			if (finished_idx == LINEAGE_BUILD) {
				// schema1: [INTEGER in_index, INTEGER out_address] TODO remove this one now that no chunking?

				LineageDataWithOffset this_data = data[LINEAGE_BUILD][data_idx];
				idx_t res_count = data[0][data_idx].data->Count();

				Vector payload(types[1], this_data.data->Process(0));
				insert_chunk.SetCardinality(res_count);
				idx_t idx = 0;

				while(idx < column_ids.size()){
					if (column_ids[idx] == 1){
						insert_chunk.data[idx].Sequence(count_so_far, 1);
					}
					else if(column_ids[idx] == 0){
						insert_chunk.data[idx].Reference(payload);
					}
					else if(column_ids[idx] == 2){
						insert_chunk.data[idx].Reference(thread_id_vec);
					}
					++idx;
				}
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else {
				// schema2: [INTEGER lhs_address, INTEGER rhs_index, INTEGER out_index]

				// This is pretty hacky, but it's fine since we're just validating that we haven't broken HashJoins
				// when introducing LineageNested
				if (cached_internal_lineage == nullptr) {
					cached_internal_lineage = make_shared<LineageNested>(
					    dynamic_cast<LineageNested &>(*data[LINEAGE_PROBE][data_idx].data)
					);
				}

				shared_ptr<LineageDataWithOffset> this_data = cached_internal_lineage->GetInternal();

				if (cached_internal_lineage->IsComplete()) {
					cached_internal_lineage = nullptr; // Clear to prepare for next LineageNested
				} else {
					data_idx--; // Subtract one since later we'll add one and we don't want to move to the next data_idx yet
				}

				Vector lhs_payload(types[0]);
				Vector rhs_payload(types[1]);

				idx_t res_count = this_data->data->Count();

				if (dynamic_cast<LineageBinary&>(*this_data->data).left == nullptr) {
					lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(lhs_payload, true);
				} else {
					Vector temp(types[0],  this_data->data->Process(0));
					lhs_payload.Reference(temp);
				}

				if (dynamic_cast<LineageBinary&>(*this_data->data).right == nullptr) {
					rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(rhs_payload, true);
				} else {
					Vector temp(types[1],  this_data->data->Process(this_data->child_offset));
					rhs_payload.Reference(temp);
				}

				insert_chunk.SetCardinality(res_count);
				idx_t idx = 0;

				while(idx < column_ids.size()){
					if (column_ids[idx] == 1){
						insert_chunk.data[idx].Reference(rhs_payload);
					}
					else if(column_ids[idx] == 0){
						insert_chunk.data[idx].Reference(lhs_payload);
					}
					else if(column_ids[idx] == 2){
						insert_chunk.data[idx].Sequence(count_so_far, 1);
					}
					else if(column_ids[idx] == 3){
						insert_chunk.data[idx].Reference(thread_id_vec);
					}
					++idx;
				}

				count_so_far += res_count;
				size_so_far += this_data->data->Size();
			}
			break;
		}
		case PhysicalOperatorType::ORDER_BY: {
			// schema: [INTEGER in_index, INTEGER out_index]
			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();

			if (res_count > STANDARD_VECTOR_SIZE) {
				D_ASSERT(data_idx == 0);
				data[LINEAGE_UNARY] = dynamic_cast<LineageSelVec *>(this_data.data.get())->Divide();
				this_data = data[LINEAGE_UNARY][0];
				res_count = this_data.data->Count();
			}

			Vector payload(types[0], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);

			idx_t idx = 0;

			while(idx < column_ids.size()){
				if (column_ids[idx] == 0){
					insert_chunk.data[idx].Sequence(count_so_far, 1);
				}
				else if(column_ids[idx] == 1){
					insert_chunk.data[idx].Reference(payload);
				}
				else if(column_ids[idx] == 2) {
					insert_chunk.data[idx].Sequence(count_so_far, 1);
				}
				++idx;
			}
			count_so_far += res_count;
			size_so_far += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			// Hash Aggregate / Perfect Hash Aggregate
			// schema for both: [INTEGER in_index, INTEGER out_index]
			if (finished_idx == LINEAGE_SINK) {
				LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector payload(types[1], this_data.data->Process(0));

				insert_chunk.SetCardinality(res_count);
				idx_t idx = 0;
				while(idx < column_ids.size()){
					if (column_ids[idx] == 0){
						insert_chunk.data[idx].Sequence(count_so_far, 1);
					}
					else if(column_ids[idx] == 1){
						insert_chunk.data[idx].Reference(payload);
					}
					else if(column_ids[idx] == 2) {
						insert_chunk.data[idx].Reference(thread_id_vec);
					}
					++idx;
				}
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else if (finished_idx == LINEAGE_COMBINE) {
				LineageDataWithOffset this_data = data[LINEAGE_COMBINE][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector source_payload(types[0], this_data.data->Process(0));
				Vector new_payload(types[1], this_data.data->Process(0));


				insert_chunk.SetCardinality(res_count);

				idx_t idx = 0;
				while(idx < column_ids.size()){
					if (column_ids[idx] == 0){
						insert_chunk.data[idx].Reference(source_payload);
					}
					else if(column_ids[idx] == 1){
						insert_chunk.data[idx].Reference(new_payload);
					}
					else if(column_ids[idx] == 2) {
						insert_chunk.data[idx].Reference(thread_id_vec);
					}
					++idx;
				}

				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			} else {
				// TODO: can we remove this one for Hash Aggregate?
				LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector payload(types[0], this_data.data->Process(0));

				insert_chunk.SetCardinality(res_count);
				idx_t idx = 0;
				while(idx < column_ids.size()){
					if (column_ids[idx] == 0){
						insert_chunk.data[idx].Sequence(count_so_far, 1);
					}
					else if(column_ids[idx] == 1){
						insert_chunk.data[idx].Reference(payload);
					}
					++idx;
				}

				//insert_chunk.data[2].Sequence(count_so_far, 1);
				count_so_far += res_count;
				size_so_far += this_data.data->Size();
			}
			break;
		}
		default:
			// We must capture lineage for everything getting processed
			D_ASSERT(false);
		}
	}
	data_idx++;
	return LineageProcessStruct{ count_so_far, size_so_far, data_idx, finished_idx, data[finished_idx].size() > data_idx };
}

void OperatorLineage::SetChunkId(idx_t idx) {
	dynamic_cast<PipelineScanLineage *>(pipeline_lineage.get())->SetChunkId(idx);
}

idx_t OperatorLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : data[0]) {
		size += lineage_data.data->Size();
	}
	for (const auto& lineage_data : data[1]) {
		size += lineage_data.data->Size();
	}
	return size;
}

shared_ptr<LineageDataWithOffset> OperatorLineage::GetMyLatest() {
	switch (type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		if (!data[0].empty()) {
			return make_shared<LineageDataWithOffset>(data[0][data[0].size() - 1]);
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		// Simple agg = ALL lineage, so child lineage data ptrs are meaningless
		return nullptr;
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY: {
		if (!data[LINEAGE_UNARY].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_UNARY][data[LINEAGE_UNARY].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::WINDOW: {
		if (!data[LINEAGE_SOURCE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_SOURCE][data[LINEAGE_SOURCE].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::CROSS_PRODUCT: {
		// Only the right lineage is ever captured TODO is this what we should do?
		if (!data[LINEAGE_PROBE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1]);
		}
		return nullptr;
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {
		// 0 is the probe side for these joins
		if (!data[0].empty()) {
			return make_shared<LineageDataWithOffset>(data[0][data[0].size() - 1]);
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// 1 is the probe side for this join
		return make_shared<LineageDataWithOffset>(data[1][data[1].size() - 1]);
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// When being asked for latest, we'll always want to refer to the probe data
		if (!data[LINEAGE_PROBE].empty()) {
			return make_shared<LineageDataWithOffset>(data[LINEAGE_PROBE][data[LINEAGE_PROBE].size() - 1]);
		} else {
			// Pass through child for Mark Hash Join TODO is this right?
			return children[LINEAGE_PROBE]->GetMyLatest();
		}
	}
	case PhysicalOperatorType::PROJECTION: {
		throw std::logic_error("We shouldn't ever try to call GetMyLatest on a Projection");
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this
		return {};
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

shared_ptr<LineageDataWithOffset> OperatorLineage::GetChildLatest(idx_t lineage_idx) {
	switch (type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::PROJECTION:
	case PhysicalOperatorType::TABLE_SCAN: {
		return nullptr;
	}
	case PhysicalOperatorType::ORDER_BY: {
		return nullptr; // Order By has no children since ALL are its children
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// Only SINK has children
		if (children.empty()) {
			// The aggregation in DelimJoin TODO figure this out
			return nullptr;
		} else if (lineage_idx == LINEAGE_SINK) {
			return children[0]->GetMyLatest();
		} else {
			return nullptr;
		}
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW: {
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// Index Join, despite being a join, just has 1 child
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// These only capture 1 lineage on PROBE side and we really care about BUILD side child
		return children[0]->GetMyLatest();
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// We mix up Hash Join...
		if (lineage_idx == LINEAGE_BUILD) {
			return children[0]->GetMyLatest();
		} else {
			return children[1]->GetMyLatest();
		}
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this
		throw std::logic_error("Haven't handled delim join yet");
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

idx_t OperatorLineage::GetThisOffset(idx_t lineage_idx) {
	idx_t last_data_idx = data[lineage_idx].size() - 1;
	return data[lineage_idx].empty() ? 0 : data[lineage_idx][last_data_idx].this_offset + data[lineage_idx][last_data_idx].data->Count();
}

LineageProcessStruct::LineageProcessStruct(idx_t i, idx_t i1, idx_t i2, idx_t i3, bool b) {
	count_so_far = i;
	size_so_far = i1;
	data_idx = i2;
	finished_idx = i3;
	still_processing = b;
}
} // namespace duckdb
#endif
