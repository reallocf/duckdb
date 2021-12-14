#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx) {
	if (!trace_lineage ) return;
	// Prepare this vector's chunk to be passed on to future operators
	pipeline_lineage->AdjustChunkOffsets(datum->Count(), lineage_idx);

	// Capture this vector
	idx_t offset = pipeline_lineage->GetChildChunkOffset(lineage_idx);

	// here global + local chunk statistics
	data[lineage_idx].push_back(LineageDataWithOffset{datum, offset});
}

void OperatorLineage::FinishedProcessing() {
	finished_idx++;
	data_idx = 0;
}

shared_ptr<PipelineLineage> OperatorLineage::GetPipelineLineage() {
	return pipeline_lineage;
}

void OperatorLineage::MarkChunkReturned() {
	dynamic_cast<PipelineJoinLineage *>(pipeline_lineage.get())->MarkChunkReturned();
}

LineageProcessStruct OperatorLineage::Process(const vector<LogicalType>& types, idx_t count_so_far,
                                              DataChunk &insert_chunk) {
	if (data[finished_idx].size() > data_idx) {
		switch (this->type) {
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::INDEX_JOIN: {
			// Index Join
			// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[0][data_idx];
			idx_t res_count = this_data.data->Count();

			Vector lhs_payload(types[0], this_data.data->Process(0)); // TODO is this right?
			Vector rhs_payload(types[1], this_data.data->Process(this_data.offset));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(lhs_payload);
			insert_chunk.data[1].Reference(rhs_payload);
			insert_chunk.data[2].Sequence(count_so_far, 1);
			count_so_far += res_count;
			break;
		}
		case PhysicalOperatorType::FILTER:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::TABLE_SCAN: {
			// Seq Scan, Filter, Limit, etc...
			// schema: [INTEGER in_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
			idx_t res_count = this_data.data->Count();

			Vector payload(types[0], this_data.data->Process(this_data.offset));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(payload);
			insert_chunk.data[1].Sequence(count_so_far, 1);
			count_so_far += res_count;
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
				insert_chunk.data[0].Sequence(count_so_far, 1);
				insert_chunk.data[1].Reference(payload);
				count_so_far += res_count;
			} else {
				// schema2: [INTEGER lhs_address, INTEGER rhs_index, INTEGER out_index]

				LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
				idx_t res_count = this_data.data->Count();
				Vector lhs_payload(types[0]);
				Vector rhs_payload(types[1]);

				if (dynamic_cast<LineageBinary&>(*this_data.data).left == nullptr) {
					lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(lhs_payload, true);
				} else {
					Vector temp(types[0],  this_data.data->Process(0));
					lhs_payload.Reference(temp);
				}

				if (dynamic_cast<LineageBinary&>(*this_data.data).right == nullptr) {
					rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
					ConstantVector::SetNull(rhs_payload, true);
				} else {
					Vector temp(types[1],  this_data.data->Process(this_data.offset));
					rhs_payload.Reference(temp);
				}

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(lhs_payload);
				insert_chunk.data[1].Reference(rhs_payload);
				insert_chunk.data[2].Sequence(count_so_far, 1);
				count_so_far += res_count;
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
			insert_chunk.data[0].Reference(payload);
			insert_chunk.data[1].Sequence(count_so_far, 1);
			count_so_far += res_count;
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
				insert_chunk.data[0].Sequence(count_so_far, 1);
				insert_chunk.data[1].Reference(payload);
				count_so_far += res_count;
			} else {
				// TODO: can we remove this one for Hash Aggregate?
				LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector payload(types[0], this_data.data->Process(0));

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(payload);
				insert_chunk.data[1].Sequence(count_so_far, 1);
				count_so_far += res_count;
			}
			break;
		}
		default:
			// We must capture lineage for everything getting processed
			D_ASSERT(false);
		}
	}
	data_idx++;
	return LineageProcessStruct{ count_so_far,data[finished_idx].size() > data_idx };
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

} // namespace duckdb
#endif
