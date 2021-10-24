//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <iostream>
#include <utility>

#ifndef QUERY_LIST_TABLE_NAME
#define QUERY_LIST_TABLE_NAME "queries_list"
#endif

#ifndef LINEAGE_UNARY

// Define meaningful lineage_idx names
#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
class ChunkLineage;
class ClientContext;
class LineageData;
class PhysicalOperator;
class PipelineLineage;


class LineageManager {
public:
	explicit LineageManager(ClientContext &context) : context(context) {};

	void AnnotatePlan(PhysicalOperator *op);
	void CreateLineageTables(PhysicalOperator *op);
	void CreateQueryTable();
	void LogQuery(const string& input_query);

private:
	ClientContext &context;
	idx_t query_id = 0;
};

struct ChunkOffset {
	idx_t offset;
	idx_t cutoff;
};

class PipelineLineage {
public:
	virtual void AddChunk(idx_t chunk_size, idx_t lineage_idx) = 0;
	virtual shared_ptr<ChunkLineage> GetChildChunkLineage(idx_t lineage_idx) = 0;
	virtual shared_ptr<ChunkLineage> GetChunkLineage() = 0;
	virtual ~PipelineLineage() {};
};

class PipelineBreakerLineage : public PipelineLineage {
public:
	explicit PipelineBreakerLineage(const shared_ptr<PipelineLineage>& child_node) : child_node(child_node) {}

	void AddChunk(idx_t chunk_size, idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChildChunkLineage(idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChunkLineage() override;

private:
	shared_ptr<PipelineLineage> child_node;
	shared_ptr<ChunkLineage> chunk_lineage;
};

class PipelineJoinLineage : public PipelineLineage {
public:
	PipelineJoinLineage(const shared_ptr<PipelineLineage>& build_child_node, const shared_ptr<PipelineLineage>& probe_child_node) :
	      build_child_node(build_child_node), probe_child_node(probe_child_node),
	      merging(false), next(true) {}

	void AddChunk(idx_t chunk_size, idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChildChunkLineage(idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChunkLineage() override;

	// Chunk Management
	void MarkChunkMerging();
	void MarkChunkNext();

private:
	shared_ptr<PipelineLineage> build_child_node;
	shared_ptr<PipelineLineage> probe_child_node;
	shared_ptr<ChunkLineage> chunk_lineage;

	bool merging;
	bool next;
};

class PipelineScanLineage : public PipelineLineage {
public:
	PipelineScanLineage() {
		// TODO we init a chunk here to properly handle index joins that don't ever use table scans, this is a hack
		SetChunkId(0);
	}

	void AddChunk(idx_t chunk_size, idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChildChunkLineage(idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChunkLineage() override;

	void SetChunkId(idx_t id);

private:
	shared_ptr<ChunkLineage> chunk_lineage;
	shared_ptr<ChunkLineage> filter_chunk_lineage;
};

class PipelineSingleLineage : public PipelineLineage {
public:
	explicit PipelineSingleLineage(shared_ptr<PipelineLineage> child_node) : child_node(move(child_node)) {}

	void AddChunk(idx_t chunk_size, idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChildChunkLineage(idx_t lineage_idx) override;
	shared_ptr<ChunkLineage> GetChunkLineage() override;

private:
	shared_ptr<PipelineLineage> child_node;
	shared_ptr<ChunkLineage> chunk_lineage;
};

class ChunkLineage {
public:
	ChunkLineage() : chunk_offset(0), chunk_size(0) {}

	void AddOffset(ChunkOffset offset);

	idx_t chunk_offset;
	idx_t chunk_size;
	vector<ChunkOffset> offsets;
};

struct LineageProcessStruct {
	idx_t count_so_far;
	bool still_processing;
};

struct LineageDataWithOffset {
	// TODO does this need to have a shared_ptr wrapper?
	shared_ptr<LineageData> data;
	shared_ptr<ChunkLineage> chunk_lineage;
};

class OperatorLineage {
public:
	explicit OperatorLineage(shared_ptr<PipelineLineage> pipeline_lineage) :
	      pipeline_lineage(move(pipeline_lineage))  {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx);
	void FinishedProcessing();
	shared_ptr<PipelineLineage> GetPipelineLineage();
	// leaky...
	void MarkChunkMerging();
	// leaky...
	void MarkChunkNext();
	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk);
	// leaky...
	void SetChunkId(idx_t idx);
	idx_t Size();

private:
	shared_ptr<PipelineLineage> pipeline_lineage;
	// data[0] used by all ops; data[1] used by pipeline breakers
	std::vector<LineageDataWithOffset> data[2];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
};

class LineageData {
public:
	virtual idx_t Count() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(vector<ChunkOffset> offsets) = 0;
	virtual idx_t Size() = 0;
	virtual ~LineageData() {};
};

// TODO get templating working like before - that would be better
class LineageDataRowVector : public LineageData {
public:
	LineageDataRowVector(vector<row_t> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	vector<row_t> vec;
	idx_t count;
};

class LineageDataUIntPtrArray : public LineageData {
public:
	LineageDataUIntPtrArray(unique_ptr<uintptr_t[]> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	unique_ptr<uintptr_t[]> vec;
	idx_t count;
};

class LineageDataUInt32Array : public LineageData {
public:
	LineageDataUInt32Array(unique_ptr<uint32_t[]>vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	unique_ptr<uint32_t[]> vec;
	idx_t count;
};

class LineageSelVec : public LineageData {
public:
	LineageSelVec(const SelectionVector& vec_p, idx_t count) : vec(vec_p), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	SelectionVector vec;
	idx_t count;
};

// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:
	LineageRange(idx_t start, idx_t end) : start(start), end(end) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	idx_t start;
	idx_t end;
	vector<sel_t> vec;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinary : public LineageData {
public:
	LineageBinary(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      left(move(lhs)), right(move(rhs)) {
		D_ASSERT(left->Count() == right->Count());
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(vector<ChunkOffset> offsets) override;
	idx_t Size() override;

private:
	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
	bool switch_on_left = true;
};


} // namespace duckdb
#endif
