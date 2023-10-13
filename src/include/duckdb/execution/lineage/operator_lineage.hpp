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
  }

	//void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1, idx_t child_offset=0);
	void CaptureUnq(unique_ptr<LineageData> datum, idx_t lineage_idx, idx_t child_offset=0);

	virtual LineageProcessStruct GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size= 0, int thread_id= -1, idx_t data_idx = 0, idx_t stage_idx = 0);

	virtual idx_t Size();
	virtual idx_t Count();
	virtual idx_t ChunksCount();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);
	virtual void BuildIndexes();

public:
	idx_t opid;
	bool trace_lineage;
	ChunkCollection chunk_collection;
	// data[0] used by all ops; data[1] used by pipeline breakers
	// Lineage data in here!
	std::vector<LineageDataWithOffset> data[4];
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

struct filter_artifact {
  unique_ptr<sel_t[]> sel;
 // buffer_ptr<SelectionData> sel;
  uint32_t count;
  idx_t child_offset;
};

class FilterLineage : public OperatorLineage {
  public:
    FilterLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;


public:
  idx_t thread_id;
  vector<filter_artifact> lineage;
};

class OrderByLineage : public OperatorLineage {
  public:
    OrderByLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;


public:
  idx_t thread_id;
  vector<vector<idx_t>> lineage;
};


struct hj_probe_artifact {
  unique_ptr<sel_t[]> left;
  unique_ptr<uintptr_t[]> right;
  uint32_t count;
  idx_t out_offset;
};

struct hj_build_artifact {
  unique_ptr<data_t[]> scatter;
  uint32_t count;
};

class HashJoinLineage : public OperatorLineage {
  public:
    HashJoinLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<hj_build_artifact> lineage_build;
  vector<hj_probe_artifact> lineage_binary;
  vector<idx_t> output_index;
  vector<idx_t> cached_output_index;
};

struct IJ_artifact {
  SelectionVector left;
  vector<row_t> right;
  uint32_t count;
  idx_t child_offset;
};

class IndexJoinLineage : public OperatorLineage {
  public:
    IndexJoinLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<IJ_artifact> lineage;
};

struct cross_artifact {
  uint32_t right_position;
  uint32_t left_chunk;
  idx_t out_start;
};

class CrossLineage : public OperatorLineage {
  public:
    CrossLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }

    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<cross_artifact> lineage;
};

struct nlj_artifact {
  SelectionVector left;
  SelectionVector right;
  uint32_t count;
  idx_t out_start;
};

class NLJLineage : public OperatorLineage {
  public:
    NLJLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<nlj_artifact> lineage;
};

struct bnlj_artifact {
  uint32_t left_position;
  SelectionVector match_sel;
  uint32_t count;
  uint32_t right_position;
  idx_t out_start;
};

class BNLJLineage : public OperatorLineage {
  public:
    BNLJLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }

    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<bnlj_artifact> lineage;
};

struct merge_artifact {
  SelectionVector left;
  SelectionVector right;
  uint32_t count;
  uint32_t right_chunk_index;
  idx_t out_start;
};

class MergeLineage : public OperatorLineage {
  public:
    MergeLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
    
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<merge_artifact> lineage;
};

struct pha_scan_artifact {
  unique_ptr<uint32_t[]> gather;
  uint32_t count;
};

class PHALineage : public OperatorLineage {
  public:
    PHALineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }

    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<vector<uint32_t>> build_lineage;
  vector<pha_scan_artifact> scan_lineage;
};

struct hg_artifact {
  unique_ptr<data_t[]> addchunk_lineage;
  uint32_t count;
};

struct flushmove_artifact {
  unique_ptr<data_t[]> src;
  unique_ptr<data_t[]> sink;
  uint32_t count;
};

struct sink_artifact {
  uint32_t branch;
  idx_t lsn;
};

struct partition_artifact {
  uint32_t partition;
  flushmove_artifact* la;
};

struct radix_artifact {
  uint32_t partition;
  SelectionVector sel;
  uint32_t sel_size;
  hg_artifact* scatter;
};

struct finalize_artifact {
  uint32_t partition;
  vector<flushmove_artifact*>* combine;
};

class HALineage : public OperatorLineage {
  public:
    HALineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }
	  
    idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<hg_artifact> addchunk_log;
  vector<sink_artifact> sink_log;
  vector<flushmove_artifact> flushmove_log;
  vector<partition_artifact> partition_log;
  vector<vector<radix_artifact>> radix_log;
  vector<vector<flushmove_artifact*>> combine_log;
  vector<finalize_artifact> finalize_log;
  vector<hg_artifact> scan_log;
};

struct scan_artifact {
  buffer_ptr<SelectionData> sel;
  uint32_t count;
  idx_t start;
  idx_t vector_index;
};


class TableScanLineage : public OperatorLineage {
  public:
    TableScanLineage(PhysicalOperatorType type, idx_t opid, idx_t thread_id) :
      OperatorLineage({}, type, opid, false), thread_id(thread_id) {
    }

	  idx_t Size() override;
	  idx_t Count() override;
	  idx_t ChunksCount() override;
	  void BuildIndexes() override;

public:
  idx_t thread_id;
  vector<scan_artifact> lineage;
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
