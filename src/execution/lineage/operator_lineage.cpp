#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
class BlockwiseNLJoinLineage : public OperatorLineage {
public:
	explicit BlockwiseNLJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// sink: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);
		sink.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(sink));
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
		idx_t res_count = this_data.data->Count();

		// constant value
		Vector lhs_payload(Value::Value::INTEGER(this_data.offset+((int*)this_data.data->Process(0))[0]));
		Vector rhs_payload(types[1], this_data.data->Process(0));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(lhs_payload);
		insert_chunk.data[1].Reference(rhs_payload);
		insert_chunk.data[2].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class CrossProductLineage : public OperatorLineage {
public:
	explicit CrossProductLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// sink: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);
		sink.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(sink));
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
		idx_t res_count = this_data.data->Count();
		// constant value
		Vector rhs_payload(Value::Value::INTEGER(((int*)this_data.data->Process(0))[0]));
		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Sequence(this_data.offset, 1);
		insert_chunk.data[1].Reference(rhs_payload);
		insert_chunk.data[2].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class DelimJoinLineage : public OperatorLineage {
public:
	explicit DelimJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// Table creation handled separately for DelimJoin
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// No-op - also handled separately for DelimJoin
		return count_so_far;
	}
};

class FilterOperatorLineage : public OperatorLineage {
public:
	explicit FilterOperatorLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineSingleLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER in_index, INTEGER out_index]

		LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
		idx_t res_count = this_data.data->Count();

		Vector payload(types[0], this_data.data->Process(this_data.offset));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(payload);
		insert_chunk.data[1].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class HashGroupByLineage : public OperatorLineage {
public:
	explicit HashGroupByLineage() {
		this->pipeline_lineage = make_shared<PipelineBreakerLineage>();

		// sink schema: [INTEGER in_index, BIGINT out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::BIGINT);
		lineage_tables_columns.emplace_back(move(sink_table_columns));
		// source schema: [BIGINT in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::BIGINT);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(source_table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		idx_t res_count;

		// schema for both: [INTEGER in_index, INTEGER out_index]
		if (finished_idx == LINEAGE_SINK) {
			LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
			res_count = this_data.data->Count();

			Vector payload(types[1], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Sequence(count_so_far, 1);
			insert_chunk.data[1].Reference(payload);
		} else {
			// TODO: can we remove this one?
			LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
			res_count = this_data.data->Count();

			Vector payload(types[0], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(payload);
			insert_chunk.data[1].Sequence(count_so_far, 1);
		}
		return count_so_far + res_count;
	}
};

class HashJoinLineage : public OperatorLineage {
public:
	explicit HashJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// build schema: [INTEGER in_index, BIGINT out_address] TODO convert from address to number?
		vector<ColumnDefinition> build_table_columns;
		build_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		build_table_columns.emplace_back("out_address", LogicalType::BIGINT);
		lineage_tables_columns.emplace_back(move(build_table_columns));
		// probe schema: [BIGINT lhs_address, INTEGER rhs_index, INTEGER out_index]
		vector<ColumnDefinition> probe_table_columns;
		probe_table_columns.emplace_back("lhs_address", LogicalType::BIGINT);
		probe_table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		probe_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(probe_table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		idx_t res_count;

		if (finished_idx == LINEAGE_BUILD) {
			// schema1: [INTEGER in_index, INTEGER out_address] TODO remove this one now that no chunking?

			LineageDataWithOffset this_data = data[LINEAGE_BUILD][data_idx];
			res_count = data[0][data_idx].data->Count();

			Vector payload(types[1], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Sequence(count_so_far, 1);
			insert_chunk.data[1].Reference(payload);
		} else {
			// schema2: [INTEGER lhs_address, INTEGER rhs_index, INTEGER out_index]

			LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
			res_count = this_data.data->Count();
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
		}
		return count_so_far + res_count;
	}
};

class IndexJoinLineage : public OperatorLineage {
public:
	explicit IndexJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::BIGINT);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		LineageDataWithOffset this_data = data[0][data_idx];
		idx_t res_count = this_data.data->Count();

		Vector lhs_payload(types[0], this_data.data->Process(0)); // TODO is this right?
		Vector rhs_payload(types[1], this_data.data->Process(this_data.offset));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(lhs_payload);
		insert_chunk.data[1].Reference(rhs_payload);
		insert_chunk.data[2].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class LimitOperatorLineage : public OperatorLineage {
public:
	explicit LimitOperatorLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineSingleLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER in_index, INTEGER out_index]
		LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
		idx_t res_count = this_data.data->Count();

		Vector payload(types[0], this_data.data->Process(this_data.offset));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(payload);
		insert_chunk.data[1].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class NestedLoopJoinLineage : public OperatorLineage {
public:
	explicit NestedLoopJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// sink: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);
		sink.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(sink));
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

		LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
		idx_t res_count = this_data.data->Count();


		Vector lhs_payload(types[1], this_data.data->Process(this_data.offset));
		// sink side, offset is adjusted during capture
		Vector rhs_payload(types[0], this_data.data->Process(0));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(lhs_payload);
		insert_chunk.data[1].Reference(rhs_payload);
		insert_chunk.data[2].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class OrderByLineage : public OperatorLineage {
public:
	explicit OrderByLineage() {
		this->pipeline_lineage = make_shared<PipelineBreakerLineage>();

		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
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
		return count_so_far + res_count;
	}
};

class PerfectHashGroupByOperatorLineage : public OperatorLineage {
public:
	explicit PerfectHashGroupByOperatorLineage() {
		this->pipeline_lineage = make_shared<PipelineBreakerLineage>();

		// sink schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(source_table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		idx_t res_count;

		// schema for both: [INTEGER in_index, INTEGER out_index]
		if (finished_idx == LINEAGE_SINK) {
			LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
			res_count = this_data.data->Count();

			Vector payload(types[1], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Sequence(count_so_far, 1);
			insert_chunk.data[1].Reference(payload);
		} else {
			LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
			res_count = this_data.data->Count();

			Vector payload(types[0], this_data.data->Process(0));

			insert_chunk.SetCardinality(res_count);
			insert_chunk.data[0].Reference(payload);
			insert_chunk.data[1].Sequence(count_so_far, 1);
		}
		return count_so_far + res_count;
	}
};

class PiecewiseMergeJoinLineage : public OperatorLineage {
public:
	explicit PiecewiseMergeJoinLineage(PhysicalOperator *op) {
		this->pipeline_lineage = make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());

		// sink: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);
		sink.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(sink));
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

		LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
		idx_t res_count = this_data.data->Count();


		Vector lhs_payload(types[1], this_data.data->Process(this_data.offset));
		// sink side, offset is adjusted during capture
		Vector rhs_payload(types[0], this_data.data->Process(0));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(lhs_payload);
		insert_chunk.data[1].Reference(rhs_payload);
		insert_chunk.data[2].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class ProjectionLineage : public OperatorLineage {
public:
	explicit ProjectionLineage(PhysicalOperator *op) {
		// Pass through to last operator
		this->pipeline_lineage = op->children[0]->lineage_op->GetPipelineLineage();
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// No-op for Projection
		return count_so_far;
	}
};

// DELIM_SCAN / CHUNK_SCAN / TABLE_SCAN
class ScanOperatorLineage : public OperatorLineage {
public:
	explicit ScanOperatorLineage() {
		this->pipeline_lineage = make_shared<PipelineScanLineage>();

		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		lineage_tables_columns.emplace_back(move(table_columns));
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// schema: [INTEGER in_index, INTEGER out_index]

		LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
		idx_t res_count = this_data.data->Count();

		Vector payload(types[0], this_data.data->Process(this_data.offset));

		insert_chunk.SetCardinality(res_count);
		insert_chunk.data[0].Reference(payload);
		insert_chunk.data[1].Sequence(count_so_far, 1);
		return count_so_far + res_count;
	}
};

class UnimplementedOperatorLineage : public OperatorLineage {
public:
	explicit UnimplementedOperatorLineage() {
		// TODO implement all of these?
	}

	idx_t InternalProcess(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) override {
		// We must capture lineage for everything getting processed
		D_ASSERT(false);
		return 0;
	}
};

shared_ptr<OperatorLineage> ConstructOperatorLineage(PhysicalOperator *op) {
	switch (op->type) {
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
		return make_shared<BlockwiseNLJoinLineage>(BlockwiseNLJoinLineage(op));
	}
	case PhysicalOperatorType::CROSS_PRODUCT: {
		return make_shared<CrossProductLineage>(CrossProductLineage(op));
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		return make_shared<DelimJoinLineage>(DelimJoinLineage(op));
	}
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		return make_shared<ScanOperatorLineage>(ScanOperatorLineage());
	}
	case PhysicalOperatorType::FILTER: {
		return make_shared<FilterOperatorLineage>(FilterOperatorLineage(op));
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		return make_shared<HashGroupByLineage>(HashGroupByLineage());
	}
	case PhysicalOperatorType::HASH_JOIN: {
		return make_shared<HashJoinLineage>(HashJoinLineage(op));
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		return make_shared<IndexJoinLineage>(IndexJoinLineage(op));
	}
	case PhysicalOperatorType::LIMIT: {
		return make_shared<LimitOperatorLineage>(LimitOperatorLineage(op));
	}
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		return make_shared<NestedLoopJoinLineage>(NestedLoopJoinLineage(op));
	}
	case PhysicalOperatorType::ORDER_BY: {
		return make_shared<OrderByLineage>(OrderByLineage());
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		return make_shared<PerfectHashGroupByOperatorLineage>(PerfectHashGroupByOperatorLineage());
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		return make_shared<PiecewiseMergeJoinLineage>(PiecewiseMergeJoinLineage(op));
	}
	case PhysicalOperatorType::PROJECTION: {
		return make_shared<ProjectionLineage>(ProjectionLineage(op));
	}
	default: {
		// Lineage not handled for this Op
		return make_shared<UnimplementedOperatorLineage>(UnimplementedOperatorLineage());
	}
	}
}

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx) {
	if (!trace_lineage ) {
		return;
	}
	// Prepare this vector's chunk to be passed on to future operators
	pipeline_lineage->AdjustChunkOffsets(datum->Count(), lineage_idx);

	// Capture this vector
	idx_t offset = pipeline_lineage->GetChildChunkOffset(lineage_idx);

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
		count_so_far = this->InternalProcess(types, count_so_far, insert_chunk);
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
