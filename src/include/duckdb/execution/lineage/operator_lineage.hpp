//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/operator_lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/enums/join_type.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/lineage/lineage_data.hpp"

#include <forward_list>
#include <iostream>
#include <utility>

#ifndef LINEAGE_UNARY

// Define meaningful lineage_idx names
#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_COMBINE 2
#define LINEAGE_FINALIZE 3
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
enum class PhysicalOperatorType : uint8_t;
struct LineageDataWithOffset;
struct LineageProcessStruct;

struct SourceAndMaybeData {
	idx_t source;
	shared_ptr<LineageDataWithOffset> data;
};

class OperatorLineage {
public:
	explicit OperatorLineage(
		std::vector<shared_ptr<OperatorLineage>> children,
	    PhysicalOperatorType type,
	    idx_t opid,
	    bool should_index
	) : opid(opid), type(type), children(move(children)), should_index(should_index) {
   /* log.resize(4);
    log[0].emplace_back(); // Create an empty partition
    log[1].emplace_back(); // Create an empty partition
    log[2].emplace_back(); // Create an empty partition
    log[3].emplace_back();*/ // Create an empty partition
  }

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1, idx_t child_offset=0);

	LineageProcessStruct GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size= 0, int thread_id= -1, idx_t data_idx = 0, idx_t stage_idx = 0);

	idx_t Size();
	idx_t Count();
	idx_t ChunksCount();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);
	void BuildIndexes();

public:
	idx_t opid;
	bool trace_lineage;
	ChunkCollection chunk_collection;
	// data[0] used by all ops; data[1] used by pipeline breakers
	// Lineage data in here!
	std::vector<LineageDataWithOffset> data[4];
	LineageDataWithOffset data_single[4];
	//std::forward_list<LineageDataWithOffset> data_test[4];
  //std::vector<std::vector<std::vector<LineageDataWithOffset>>> log;
	idx_t op_offset[4];
	PhysicalOperatorType type;
	shared_ptr<LineageVec> cached_internal_lineage = nullptr;
	std::vector<shared_ptr<OperatorLineage>> children;
    bool should_index;
	JoinType join_type;

  /*  Indexes */
  // index: used to index selection vectors
  //        it stores the size of SV from each chunk
  //        which helps in locating the one needed
  //        using binary-search.
  // Index for when we need to identify the chunk from a global offset
  vector<idx_t> index;

	// Index for hash aggregate
  std::unordered_map<idx_t, vector<SourceAndMaybeData>> hash_map_agg;
	// hash_chunk_count: maintain count of data that belong to previous ranges
	vector<idx_t> hash_chunk_count;
	// hm_range: maintains the existing ranges in hash join build side
	std::vector<std::pair<idx_t, idx_t>> hm_range;
};

struct LineageProcessStruct {
	LineageProcessStruct(idx_t i, idx_t i1, idx_t i2, idx_t i3, bool b);
	idx_t count_so_far;
	idx_t size_so_far;
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
	bool still_processing;
};

} // namespace duckdb
#endif
