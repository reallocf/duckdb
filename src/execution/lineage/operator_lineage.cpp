#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id, idx_t child_offset) {
	if (!trace_lineage || datum->Count() == 0) return;

	// Set child ptr
	datum->SetChild(GetChildLatest(lineage_idx));

	idx_t this_offset = op_offset[lineage_idx];
	op_offset[lineage_idx] += datum->Count();
	data[lineage_idx].push_back(LineageDataWithOffset{datum, (int)child_offset, this_offset});
}

void fillBaseChunk(DataChunk &insert_chunk, idx_t res_count, Vector &lhs_payload, Vector &rhs_payload, idx_t count_so_far, Vector &thread_id_vec) {
	insert_chunk.SetCardinality(res_count);
	insert_chunk.data[0].Reference(lhs_payload);
	insert_chunk.data[1].Reference(rhs_payload);
	insert_chunk.data[2].Sequence(count_so_far, 1);
	insert_chunk.data[3].Reference(thread_id_vec);
}

LineageProcessStruct OperatorLineage::GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
                                              DataChunk &insert_chunk, idx_t size, int thread_id, idx_t data_idx, idx_t stage_idx) {
	if (data[stage_idx].size() > data_idx) {
		Vector thread_id_vec(Value::INTEGER(thread_id));
		switch (this->type) {
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			// Seq Scan, Filter, Limit, Order By, TopN, etc...
			// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();

			if (res_count > STANDARD_VECTOR_SIZE) {
				D_ASSERT(data_idx == 0);
				data[LINEAGE_UNARY] =
				    dynamic_cast<LineageSelVec *>(this_data.data.get())->Divide(this_data.child_offset);
				this_data = data[LINEAGE_UNARY][0];
				res_count = this_data.data->Count();
			}
			insert_chunk.Reset();
			insert_chunk.SetCardinality(res_count);
			Vector in_index = this_data.data->GetVecRef(types[0], this_data.child_offset);
			insert_chunk.data[0].Reference(in_index);
			insert_chunk.data[1].Sequence(count_so_far, 1); // out_index
			insert_chunk.data[2].Reference(thread_id_vec);  // thread_id

			count_so_far += res_count;
			size += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();
			Vector rhs_payload = this_data.data->GetVecRef(types[0], 0);
			Vector lhs_payload(types[1], res_count);
			lhs_payload.Sequence(this_data.child_offset, 1);
			fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);
			count_so_far += res_count;
			size += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::INDEX_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			idx_t res_count = this_data.data->Count();

			Vector lhs_payload = dynamic_cast<LineageBinary&>(*this_data.data).left->GetVecRef(types[0], this_data.child_offset);
			Vector rhs_payload = dynamic_cast<LineageBinary&>(*this_data.data).right->GetVecRef(types[1], 0);
			fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);
			count_so_far += res_count;
			size += this_data.data->Size();
			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
			// Hash Join - other joins too?
			if (stage_idx == LINEAGE_BUILD) {
				// sink: [BIGINT in_index, INTEGER out_index, INTEGER thread_id]
				LineageDataWithOffset this_data = data[LINEAGE_BUILD][data_idx];
				idx_t res_count = data[0][data_idx].data->Count();

				Vector payload = this_data.data->GetVecRef(types[1], 0);
				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Sequence(count_so_far, 1);
				insert_chunk.data[1].Reference(payload);
				insert_chunk.data[2].Reference(thread_id_vec);

				count_so_far += res_count;
				size += this_data.data->Size();
			} else {
				// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
        auto pdata = data[LINEAGE_PROBE][data_idx].data;

				// This is pretty hacky, but it's fine since we're just validating that we haven't broken HashJoins
				// when introducing LineageNested
				if (cached_internal_lineage == nullptr && (typeid(*pdata) == typeid(LineageVec))) {
            cached_internal_lineage = make_shared<LineageVec>(dynamic_cast<LineageVec &>(*pdata));
				}

        LineageDataWithOffset this_data;
        if (cached_internal_lineage) {
          this_data = cached_internal_lineage->GetInternal();

          if (cached_internal_lineage->IsComplete()) {
            cached_internal_lineage = nullptr; // Clear to prepare for next LineageNested
          } else {
            data_idx--; // Subtract one since later we'll add one and we don't want to move to the next data_idx yet
          }
        } else {
          this_data = data[LINEAGE_PROBE][data_idx];
        }

				Vector lhs_payload(types[0]);
				Vector rhs_payload(types[1]);

				idx_t res_count = this_data.data->Count();

				// Left side / probe side
				if (dynamic_cast<LineageBinary&>(*this_data.data).left == nullptr) {
					lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(lhs_payload, true);
				} else {
					Vector temp(types[0],  this_data.data->Process(this_data.child_offset));
					lhs_payload.Reference(temp);
				}

				// Right side / build side
				if (dynamic_cast<LineageBinary&>(*this_data.data).right == nullptr) {
					rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(rhs_payload, true);
				} else {
					Vector temp(types[1],  this_data.data->Process(0));
					rhs_payload.Reference(temp);
				}

				fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id_vec);

				count_so_far += res_count;
				size += this_data.data->Size();
			}
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			// Hash Aggregate / Perfect Hash Aggregate
			// sink schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
			if (stage_idx == LINEAGE_SINK) {
				// in_index | LogicalType::INTEGER, out_index|LogicalType::BIGINT, thread_id|LogicalType::INTEGER
				LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector out_index = this_data.data->GetVecRef(types[1], 0);

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Sequence(count_so_far, 1);
				insert_chunk.data[1].Reference(out_index);
				insert_chunk.data[2].Reference(thread_id_vec);

				count_so_far += res_count;
				size += this_data.data->Size();
			} else if (stage_idx == LINEAGE_COMBINE) {
				LineageDataWithOffset this_data = data[LINEAGE_COMBINE][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector source_payload(types[0], this_data.data->Process(0));
				Vector new_payload(types[1], this_data.data->Process(0));


				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(source_payload);
				insert_chunk.data[1].Reference(new_payload);
				insert_chunk.data[2].Reference(thread_id_vec);


				count_so_far += res_count;
				size += this_data.data->Size();
			} else {
				// in_index|LogicalType::BIGINT, out_index|LogicalType::INTEGER, thread_id| LogicalType::INTEGER
				LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
				idx_t res_count = this_data.data->Count();

				//Vector in_index(types[0], this_data.data->GetLineageAsChunk(0));
				Vector in_index = this_data.data->GetVecRef(types[0], 0);
				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(in_index);
				insert_chunk.data[1].Sequence(count_so_far, 1); // out_index
				insert_chunk.data[2].Reference(thread_id_vec);

				count_so_far += res_count;
				size += this_data.data->Size();
			}
			break;
		}
		default:
			// We must capture lineage for everything getting processed
			D_ASSERT(false);
		}
	}
	data_idx++;
	return LineageProcessStruct{ count_so_far, size, data_idx, stage_idx, data[stage_idx].size() > data_idx };
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
idx_t OperatorLineage::Count() {
  idx_t size = 0;
  for (const auto& lineage_data : data[0]) {
    size += lineage_data.data->Count();
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

LineageProcessStruct::LineageProcessStruct(idx_t i, idx_t i1, idx_t i2, idx_t i3, bool b) {
	count_so_far = i;
	size_so_far = i1;
	data_idx = i2;
	finished_idx = i3;
	still_processing = b;
}

void OperatorLineage::BuildIndexes() {
		switch (this->type) {
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::INDEX_JOIN:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
      // Binary Search Index
			auto size = data[LINEAGE_UNARY].size();
			index.reserve(size);
      idx_t count_so_far = 0;
      // O(number of chunks)
      for (auto i=0; i < size; ++i) {
			  count_so_far += data[LINEAGE_UNARY][i].data->Count();
				index.push_back(count_so_far);
      }

			break;
		}
		case PhysicalOperatorType::HASH_JOIN: {
      // Binary Search Index
			auto size = data[LINEAGE_PROBE].size();
			index.reserve(size);
      idx_t count_so_far = 0;
      // O(number of chunks)
      for (auto i=0; i < size; ++i) {
			  auto pdata = data[LINEAGE_PROBE][i].data;
				if (typeid(*pdata) == typeid(LineageVec)) {
          count_so_far += dynamic_cast<LineageVec&>(*pdata).BuildInnerIndex();
        } else {
			    count_so_far += data[LINEAGE_PROBE][i].data->Count();
        }
				index.push_back(count_so_far);
      }

			size = data[LINEAGE_BUILD].size();
      idx_t start_base = 0;
      idx_t last_base = 0;
      uint64_t offset = 0;
      if (size > 0) {
        LineageDataWithOffset this_data = data[LINEAGE_BUILD][0];
        auto payload = (uint64_t*)this_data.data->Process(0);
        idx_t res_count = this_data.data->Count();
        start_base = payload[0];
        last_base = payload[res_count - 1];
        hm_range.emplace_back(start_base, last_base);
        hash_chunk_count.push_back(0);
        if (offset == 0 && res_count > 1) {
          offset = payload[1] - payload[0];
        }
      }

      for (auto i=1; i < size; ++i) {
        // build hash table with range -> acc
        // if x in range -> then use range.start and adjust the value using acc
        LineageDataWithOffset this_data = data[LINEAGE_BUILD][i];
        auto payload = (uint64_t*)this_data.data->Process(0);
        idx_t count_so_far = this_data.this_offset;
        idx_t res_count = this_data.data->Count();
        if (offset == 0) offset = payload[res_count - 1] - start_base;
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
			break;
		}
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
			// Hash Aggregate / Perfect Hash Aggregate
			auto size = data[LINEAGE_SINK].size();
      idx_t count_so_far = 0;
			// build hash table
      if (type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY) {
        for (auto i=0; i < size; i++) {
          LineageDataWithOffset this_data = data[LINEAGE_SINK][i];
          idx_t res_count = this_data.data->Count();
          auto payload = (sel_t*)this_data.data->Process(0);
          for (idx_t j=0; j < res_count; ++j) {
            hash_map_agg[(idx_t)payload[j]].push_back({j + count_so_far, nullptr});
          }
          count_so_far += res_count;
        }
      } else {
        for (auto i=0; i < size; i++) {
          LineageDataWithOffset this_data = data[LINEAGE_SINK][i];
          idx_t res_count = this_data.data->Count();
          auto payload = (uint64_t*)this_data.data->Process(0);
          for (idx_t j=0; j < res_count; ++j) {
            hash_map_agg[(idx_t)payload[j]].push_back({j + count_so_far, nullptr});
          }
          count_so_far += res_count;
        }
      }

      // Scan side
      // Binary Search Index
			size = data[LINEAGE_SOURCE].size();
			index.reserve(size);
      count_so_far = 0;
      // O(number of chunks)
      for (auto i=0; i < size; ++i) {
			  count_so_far += data[LINEAGE_UNARY][i].data->Count();
				index.push_back(count_so_far);
      }
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
      // Binary Search Index
			auto size = data[LINEAGE_PROBE].size();
			index.reserve(size);
      idx_t count_so_far = 0;
      // O(number of chunks)
      for (auto i=0; i < size; ++i) {
			  count_so_far += data[LINEAGE_PROBE][i].data->Count();
				index.push_back(count_so_far);
      }
			break;
		}
		default:
			// We must capture lineage for everything getting post-processed
			D_ASSERT(false);
		}
}

} // namespace duckdb
#endif
