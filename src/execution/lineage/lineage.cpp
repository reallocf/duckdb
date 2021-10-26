#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <utility>

namespace duckdb {
class PhysicalOperator;

// LineageManager

shared_ptr<PipelineLineage> GetPipelineLineageNodeForOp(PhysicalOperator *op) {
	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		return make_shared<PipelineScanLineage>();
	}
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER: {
		return make_shared<PipelineSingleLineage>(op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY: {
		return make_shared<PipelineBreakerLineage>(op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::HASH_JOIN: {
		return make_shared<PipelineJoinLineage>(op->children[1]->lineage_op->GetPipelineLineage(),
									 op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::PROJECTION: {
		// Pass through to last operator
		return op->children[0]->lineage_op->GetPipelineLineage();
	}
	default:
		// Lineage unimplemented! TODO these :)
		return nullptr;
	}
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
idx_t PlanAnnotator(PhysicalOperator *op, idx_t counter) {
	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}
	op->id = counter;
	op->lineage_op = make_shared<OperatorLineage>(GetPipelineLineageNodeForOp(op));
	return counter + 1;
}

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 */
void LineageManager::AnnotatePlan(PhysicalOperator *op) {
	PlanAnnotator(op, 0);
}

// Get the column types for this operator
// Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN: {
		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source_table_columns));
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::BIGINT);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// build schema: [INTEGER in_index, BIGINT out_address] TODO convert from address to number?
		vector<ColumnDefinition> build_table_columns;
		build_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		build_table_columns.emplace_back("out_address", LogicalType::BIGINT);
		res.emplace_back(move(build_table_columns));
		// probe schema: [BIGINT lhs_address, INTEGER rhs_index, INTEGER out_index]
		vector<ColumnDefinition> probe_table_columns;
		probe_table_columns.emplace_back("lhs_address", LogicalType::BIGINT);
		probe_table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		probe_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(probe_table_columns));
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return res;
}

// Create the table for this operator and fill it with lineage
void LineageManager::CreateLineageTables(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);

	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
							+ op->GetName() + "_" + to_string(op->id) + "_" + to_string(i);

		// Create Table
		auto info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = table_name;
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			info->columns.push_back(move(table_column_types[i][col_i]));
		}
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		TableCatalogEntry *table =
			dynamic_cast<TableCatalogEntry *>(catalog.CreateTable(context, bound_create_info.get()));

		// Persist Data
		DataChunk insert_chunk;
		vector<LogicalType> types = table->GetTypes();
		insert_chunk.Initialize(types);
		LineageProcessStruct lps = op->lineage_op->Process(table->GetTypes(), 0, insert_chunk);
		table->Persist(*table, context, insert_chunk);
		while (lps.still_processing) {
			lps = op->lineage_op->Process(table->GetTypes(), lps.count_so_far, insert_chunk);
			table->Persist(*table, context, insert_chunk);
		}
		op->lineage_op->FinishedProcessing();
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(op->children[i].get());
	}
}

/*
 * Create table to store executed queries with their IDs
 * Table name: queries_list
 * Schema: (INT query_id, BLOB query)
 */
void LineageManager::CreateQueryTable() {
	auto info = make_unique <CreateTableInfo>();
	info->schema = DEFAULT_SCHEMA;
	info->table = QUERY_LIST_TABLE_NAME;
	// This is recreated when a database is spun back up, so ignore
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;

	info->columns.emplace_back("query_id", LogicalType::INTEGER);
	info->columns.emplace_back("query", LogicalType::BLOB);

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(info));
	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateTable(context, bound_create_info.get());
}

/*
 * Persist executed query in queries_list table
 */
void LineageManager::LogQuery(const string& input_query) {
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context)
	                             .GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, QUERY_LIST_TABLE_NAME);
  DataChunk insert_chunk;
  insert_chunk.Initialize(table->GetTypes());
  insert_chunk.SetCardinality(count);

  // query id
  Vector query_ids(Value::INTEGER(++query_id));

  // query value
  Vector payload(Value::BLOB(input_query));

  // populate chunk
  insert_chunk.data[0].Reference(query_ids);
  insert_chunk.data[1].Reference(payload);

  table->Persist(*table, context, insert_chunk);
}

// PipelineBreakerLineage

void PipelineBreakerLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_SOURCE) {
		idx_t offset;
		if (chunk_offset == nullptr) {
			offset = 0;
		} else {
			offset = chunk_offset->offset + chunk_offset->size;
		}
		chunk_offset = make_shared<ChunkOffset>();
		chunk_offset->offset = offset;
		chunk_offset->size = chunk_size;
	} else {
		// We don't care about the sink's offset since it's never used
	}
}

shared_ptr<ChunkOffset> PipelineBreakerLineage::GetChildChunkOffset(idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_SINK) {
		return child_node->GetChunkOffset();
	} else {
		// For source, adjust by how many we've output so far TODO is this right?
		return chunk_offset;
	}
}

shared_ptr<ChunkOffset> PipelineBreakerLineage::GetChunkOffset() {
	return chunk_offset;
}

// PipelineJoinLineage

void PipelineJoinLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_PROBE) {
		idx_t offset;
		idx_t size;
		if (chunk_offset == nullptr) {
			offset = 0;
			size = chunk_size;
		} else if (next) {
			// If we've just pushed to the parent operator, adjust offset based on the current chunk
			offset = chunk_offset->offset + chunk_offset->size;
			size = chunk_size;
			next = false;
		} else {
			// If we haven't pushed to the parent operator, offset remains the same (chunk merge)
			offset = chunk_offset->offset;
			size = chunk_offset->size + chunk_size;
		}
		chunk_offset = make_shared<ChunkOffset>();
		chunk_offset->offset = offset;
		chunk_offset->size = size;
	} else {
		// We don't care about the build's offset since it's never used
	}
}

shared_ptr<ChunkOffset> PipelineJoinLineage::GetChildChunkOffset(idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_BUILD) {
		return build_child_node->GetChunkOffset();
	} else {
		return probe_child_node->GetChunkOffset();
	}
}

shared_ptr<ChunkOffset> PipelineJoinLineage::GetChunkOffset() {
	// Only ever called on the probe side, since build side is consumed by itself
	return chunk_offset;
}

void PipelineJoinLineage::MarkChunkReturned() {
	next = true;
}

// PipelineScanLineage

void PipelineScanLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	idx_t offset;
	if (filter_chunk_offset == nullptr) {
		offset = 0;
	} else {
		offset = filter_chunk_offset->offset + filter_chunk_offset->size;
	}
	filter_chunk_offset = make_shared<ChunkOffset>();
	filter_chunk_offset->offset = offset;
	filter_chunk_offset->size = chunk_size;
}

shared_ptr<ChunkOffset> PipelineScanLineage::GetChildChunkOffset(idx_t lineage_idx) {
	// Child lineage for Scan is adjusting the pushed down filter based on what chunk we're in
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	return chunk_offset;
}

shared_ptr<ChunkOffset> PipelineScanLineage::GetChunkOffset() {
	if (filter_chunk_offset == nullptr) {
		// TODO is this right?
		return chunk_offset;
	} else {
		return filter_chunk_offset;
	}
}

void PipelineScanLineage::SetChunkId(idx_t id) {
	chunk_offset = make_shared<ChunkOffset>();
	chunk_offset->offset = id * STANDARD_VECTOR_SIZE;
	chunk_offset->size = STANDARD_VECTOR_SIZE;
}

// PipelineSingleLineage

void PipelineSingleLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	idx_t offset;
	if (chunk_offset == nullptr) {
		offset = 0;
	} else {
		offset = chunk_offset->offset + chunk_offset->size;
	}
	chunk_offset = make_shared<ChunkOffset>();
	chunk_offset->offset = offset;
	chunk_offset->size = chunk_size;
}

shared_ptr<ChunkOffset> PipelineSingleLineage::GetChildChunkOffset(idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	return child_node->GetChunkOffset();
}

shared_ptr<ChunkOffset> PipelineSingleLineage::GetChunkOffset() {
	return chunk_offset;
}

// OperatorLineage

void OperatorLineage::Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx) {
	// Prepare this vector's chunk to be passed on to future operators
	pipeline_lineage->AdjustChunkOffsets(datum->Count(), lineage_idx);

	// Capture this vector
	shared_ptr<ChunkOffset> child_chunk_lineage = pipeline_lineage->GetChildChunkOffset(lineage_idx);
	data[lineage_idx].push_back(LineageDataWithOffset{datum, child_chunk_lineage});
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
	if (data[1].empty()) {
		// Non-Pipeline Breaker
		if (data[0].size() > data_idx) {
			if (dynamic_cast<LineageBinary *>(data[LINEAGE_UNARY][0].data.get()) != nullptr) {
				// Index Join
				// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

				LineageDataWithOffset this_data = data[0][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector lhs_payload(types[0], this_data.data->Process(0)); // TODO is this right?
				Vector rhs_payload(types[1], this_data.data->Process(this_data.chunk_lineage->offset));

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(lhs_payload);
				insert_chunk.data[1].Reference(rhs_payload);
				insert_chunk.data[2].Sequence(count_so_far, 1);
				count_so_far += res_count;
			} else {
				// Seq Scan, Filter, Limit, etc...
				// schema: [INTEGER in_index, INTEGER out_index]

				LineageDataWithOffset this_data = data[LINEAGE_UNARY][data_idx];
				idx_t res_count = this_data.data->Count();

				Vector payload(types[0], this_data.data->Process(this_data.chunk_lineage->offset));

				insert_chunk.SetCardinality(res_count);
				insert_chunk.data[0].Reference(payload);
				insert_chunk.data[1].Sequence(count_so_far, 1);
				count_so_far += res_count;
			}
		}
	} else {
		// Pipeline Breaker
		if (data[finished_idx].size() > data_idx) {
			if (dynamic_cast<LineageBinary *>(data[LINEAGE_PROBE][0].data.get()) != nullptr) {
				// Hash Join - other joins too?
				if (finished_idx == LINEAGE_BUILD) {
					// schema1: [INTEGER in_index, INTEGER out_address] TODO remove this one now that no chunking?

					LineageDataWithOffset this_data = data[LINEAGE_BUILD][data_idx];
					idx_t res_count = data[0][data_idx].data->Count();

					Vector payload(types[1], this_data.data->Process(this_data.chunk_lineage->offset));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Sequence(count_so_far, 1);
					insert_chunk.data[1].Reference(payload);
					count_so_far += res_count;
				} else {
					// schema2: [INTEGER lhs_address, INTEGER rhs_index, INTEGER out_index]

					LineageDataWithOffset this_data = data[LINEAGE_PROBE][data_idx];
					idx_t res_count = this_data.data->Count();

					Vector lhs_payload(types[0], this_data.data->Process(0));
					Vector rhs_payload(types[1], this_data.data->Process(this_data.chunk_lineage->offset));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Reference(lhs_payload);
					insert_chunk.data[1].Reference(rhs_payload);
					insert_chunk.data[2].Sequence(count_so_far, 1);
					count_so_far += res_count;
				}
			} else {
				// Hash Aggregate / Perfect Hash Aggregate
				// schema for both: [INTEGER in_index, INTEGER out_index]
				if (finished_idx == LINEAGE_SINK) {
					LineageDataWithOffset this_data = data[LINEAGE_SINK][data_idx];
					idx_t res_count = this_data.data->Count();

					Vector payload(types[1], this_data.data->Process(this_data.chunk_lineage->offset));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Sequence(count_so_far, 1);
					insert_chunk.data[1].Reference(payload);
					count_so_far += res_count;
				} else {
					// TODO: can we remove this one for Hash Aggregate?
					LineageDataWithOffset this_data = data[LINEAGE_SOURCE][data_idx];
					idx_t res_count = this_data.data->Count();

					Vector payload(types[0], this_data.data->Process(this_data.chunk_lineage->offset));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Reference(payload);
					insert_chunk.data[1].Sequence(count_so_far, 1);
					count_so_far += res_count;
				}
			}
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

// LineageDataRowVector

idx_t LineageDataRowVector::Count() {
	return count;
}

void LineageDataRowVector::Debug() {
	std::cout << "LineageDataVector " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataRowVector::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageDataRowVector::Size() {
	return count * sizeof(vec[0]);
}


// LineageDataUIntPtrArray

idx_t LineageDataUIntPtrArray::Count() {
	return count;
}

void LineageDataUIntPtrArray::Debug() {
	std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataUIntPtrArray::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.get();
}

idx_t LineageDataUIntPtrArray::Size() {
	return count * sizeof(vec[0]);
}


// LineageDataUInt32Array

idx_t LineageDataUInt32Array::Count() {
	return count;
}

void LineageDataUInt32Array::Debug() {
	std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataUInt32Array::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.get();
}

idx_t LineageDataUInt32Array::Size() {
	return count * sizeof(vec[0]);
}


// LineageSelVec

idx_t LineageSelVec::Count() {
	return count;
}

void LineageSelVec::Debug() {
	std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageSelVec::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		*(vec.data() + i) += offset;
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageSelVec::Size() {
	return count * sizeof(vec.get_index(0));
}


// LineageRange

idx_t LineageRange::Count() {
	return end - start;
}

void LineageRange::Debug() {
	std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
}

data_ptr_t LineageRange::Process(idx_t offset) {
	// Lazily convert lineage range to selection vector
	if (vec.empty()) {
		for (idx_t i = start; i < end; i++) {
			vec.push_back(i + offset);
		}
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageRange::Size() {
	return 2*sizeof(start);
}


// LineageBinary

idx_t LineageBinary::Count() {
	return left->Count();
}

void LineageBinary::Debug() {
	left->Debug();
	right->Debug();
}

data_ptr_t LineageBinary::Process(idx_t offset) {
	if (switch_on_left) {
		switch_on_left = !switch_on_left;
		return left->Process(offset);
	} else {
		switch_on_left = !switch_on_left;
		return right->Process(offset);
	}
}

idx_t LineageBinary::Size() {
	return left->Size() + right->Size();
}


} // namespace duckdb
#endif
