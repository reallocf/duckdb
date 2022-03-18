//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator_lineage.hpp
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
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/execution/lineage/pipeline_lineage.hpp"

#include <iostream>
#include <utility>

#ifndef LINEAGE_UNARY

// Define meaningful lineage_idx names
#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_COMBINE 2
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
class LineageRes;
enum class PhysicalOperatorType : uint8_t;
struct LineageDataWithOffset;
struct LineageProcessStruct;
struct SimpleAggQueryStruct;
struct SourceAndMaybeData;

class OperatorLineage {
public:
	explicit OperatorLineage(
		shared_ptr<PipelineLineage> pipeline_lineage,
		std::vector<shared_ptr<OperatorLineage>> children,
	    PhysicalOperatorType type,
	    idx_t opid,
	    bool should_index
	) : opid(opid), pipeline_lineage(move(pipeline_lineage)), type(type), children(move(children)), should_index(should_index) {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1);

	void FinishedProcessing();
	shared_ptr<PipelineLineage> GetPipelineLineage();
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void MarkChunkReturned();
	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size=0, int thread_id=-1);
	LineageProcessStruct PostProcess(idx_t chunk_count, idx_t count_so_far, int thread_id=-1);
	unique_ptr<LineageRes> Backward(unique_ptr<vector<SourceAndMaybeData>> lineage);
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void SetChunkId(idx_t idx);
	idx_t Size();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);
	idx_t GetThisOffset(idx_t lineage_idx);
	SimpleAggQueryStruct RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child);

public:
	idx_t opid;
	bool trace_lineage;
	shared_ptr<PipelineLineage> pipeline_lineage;
	// data[0] used by all ops; data[1] used by pipeline breakers
	std::vector<LineageDataWithOffset> data[3];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
	PhysicalOperatorType type;
	shared_ptr<LineageNested> cached_internal_lineage = nullptr;
	std::vector<shared_ptr<OperatorLineage>> children;
	// final lineage indexing data-structures
	// hash_chunk_count: maintain count of data that belong to previous ranges
	vector<idx_t> hash_chunk_count;
	// hm_range: maintains the existing ranges in hash join build side
	std::vector<std::pair<idx_t, idx_t>> hm_range;
	// offset: difference between two consecutive values with a range
	uint64_t offset = 0;
	idx_t start_base = 0;
	idx_t last_base = 0;

   std::unordered_map<idx_t, vector<SourceAndMaybeData>> hash_map_agg;
   // index: used to index selection vectors
   //        it stores the size of SV from each chunk
   //        which helps in locating the one needed
   //        using binary-search.
   vector<idx_t> index;
   bool should_index;
};

struct LineageProcessStruct {
	idx_t count_so_far;
	idx_t size_so_far;
	bool still_processing;
};

struct SimpleAggQueryStruct {
	shared_ptr<OperatorLineage> materialized_child_op;
	vector<LineageDataWithOffset> child_lineage_data_vector;
};

struct SourceAndMaybeData {
	idx_t source;
	shared_ptr<LineageDataWithOffset> data;
};

class LineageRes {
public:
	virtual vector<idx_t> GetValues() = 0;
	virtual idx_t GetCount() = 0;
	virtual ~LineageRes() {};
};

class LineageResAgg : public LineageRes {
public:
	explicit LineageResAgg(vector<unique_ptr<LineageRes>> vals) : vals(move(vals)) {}

	vector<idx_t> GetValues() override;
	idx_t GetCount() override;

private:
	vector<unique_ptr<LineageRes>> vals;
};

class LineageResJoin : public LineageRes {
public:
	LineageResJoin(unique_ptr<LineageRes> left_val, unique_ptr<LineageRes> right_val)
	    : left_val(move(left_val)), right_val(move(right_val)) {}

	vector<idx_t> GetValues() override;
	idx_t GetCount() override;

private:
	unique_ptr<LineageRes> left_val;
	unique_ptr<LineageRes> right_val;
};

class LineageResVal : public LineageRes {
public:
	explicit LineageResVal(unique_ptr<vector<SourceAndMaybeData>> lineage) : vals(move(lineage)) {}

	vector<idx_t> GetValues() override;
	idx_t GetCount() override;

private:
	unique_ptr<vector<SourceAndMaybeData>> vals;
};

} // namespace duckdb
#endif
