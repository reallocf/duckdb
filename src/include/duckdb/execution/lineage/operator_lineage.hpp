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
enum class PhysicalOperatorType : uint8_t;
struct LineageDataWithOffset;
struct LineageProcessStruct;
struct SourceAndMaybeData;

class OperatorLineage {
public:
	explicit OperatorLineage(
		shared_ptr<PipelineLineage> pipeline_lineage,
		std::vector<shared_ptr<OperatorLineage>> children,
	    PhysicalOperatorType type,
	    bool should_index
	) : pipeline_lineage(move(pipeline_lineage)), type(type), children(move(children)), should_index(should_index) {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1);

	void FinishedProcessing();
	shared_ptr<PipelineLineage> GetPipelineLineage();
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void MarkChunkReturned();
	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size=0, int thread_id=-1);
	LineageProcessStruct PostProcess(idx_t chunk_count, idx_t count_so_far, int thread_id=-1);
	void Backward(const shared_ptr<vector<SourceAndMaybeData>>& lineage);
	shared_ptr<vector<SourceAndMaybeData>> BackwardNext(bool next_to_leaf=false);
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void SetChunkId(idx_t idx);
	idx_t Size();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);
	idx_t GetThisOffset(idx_t lineage_idx);

public:
	bool trace_lineage;
	shared_ptr<PipelineLineage> pipeline_lineage;
	// data[0] used by all ops; data[1] used by pipeline breakers
	std::vector<LineageDataWithOffset> data[3];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
	PhysicalOperatorType type;
	shared_ptr<LineageNested> cached_internal_lineage = nullptr;
	std::vector<shared_ptr<OperatorLineage>> children;
	vector<shared_ptr<OperatorLineage>> parents;

   // final lineage indexing data-structures
   // hash_map: used by group by and hash join build side
   std::unordered_map<uint64_t, SourceAndMaybeData> hash_map;
   std::unordered_map<idx_t, vector<shared_ptr<vector<SourceAndMaybeData>>>> hash_map_agg;
   // index: used to index selection vectors
   //        it stores the size of SV from each chunk
   //        which helps in locating the one needed
   //        using binary-search.
   vector<idx_t> index;
   bool should_index;

   // Lineage Querying metadata and caches
   vector<SourceAndMaybeData> sources;
   bool visited = false;
   vector<shared_ptr<vector<SourceAndMaybeData>>> cached_lineage_vec;
   idx_t cached_lineage_idx = 0;
};

struct LineageProcessStruct {
	idx_t count_so_far;
	idx_t size_so_far;
	bool still_processing;
};

struct SourceAndMaybeData {
	idx_t source;
	shared_ptr<LineageDataWithOffset> data;
};

} // namespace duckdb
#endif
