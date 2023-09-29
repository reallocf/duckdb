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

class OperatorLineage {
public:
	explicit OperatorLineage(
		std::vector<shared_ptr<OperatorLineage>> children,
	    PhysicalOperatorType type,
	    idx_t opid,
	    bool should_index
	) : opid(opid), type(type), children(move(children)), should_index(should_index) {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1, idx_t child_offset=0);

	shared_ptr<LineageDataWithOffset> ConstructNestedData(const shared_ptr<LineageData>& datum, idx_t lineage_idx, idx_t child_offset);

	LineageProcessStruct GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size= 0, int thread_id= -1, idx_t data_idx = 0, idx_t stage_idx = 0);

	idx_t Size();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);

public:
	idx_t opid;
	bool trace_lineage;
	ChunkCollection chunk_collection;
	// data[0] used by all ops; data[1] used by pipeline breakers
	// Lineage data in here!
	std::vector<LineageDataWithOffset> data[3];
	idx_t op_offset[3];
	PhysicalOperatorType type;
	shared_ptr<LineageNested> cached_internal_lineage = nullptr;
	std::vector<shared_ptr<OperatorLineage>> children;
    bool should_index;
	JoinType join_type;
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
