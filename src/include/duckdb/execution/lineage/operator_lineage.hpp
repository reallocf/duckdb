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
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
struct LineageDataWithOffset;
struct LineageProcessStruct;

class OperatorLineage {
public:
	explicit OperatorLineage(shared_ptr<PipelineLineage> pipeline_lineage) :
	      pipeline_lineage(move(pipeline_lineage))  {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx);
	void FinishedProcessing();
	shared_ptr<PipelineLineage> GetPipelineLineage();
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void MarkChunkReturned();
	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk);
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void SetChunkId(idx_t idx);
	idx_t Size();

private:
	shared_ptr<PipelineLineage> pipeline_lineage;
	// data[0] used by all ops; data[1] used by pipeline breakers
	std::vector<LineageDataWithOffset> data[2];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
};

struct LineageDataWithOffset {
	// TODO does this need to have a shared_ptr wrapper?
	shared_ptr<LineageData> data;
	idx_t offset;
};

struct LineageProcessStruct {
	idx_t count_so_far;
	bool still_processing;
};

} // namespace duckdb
#endif
