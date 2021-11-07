//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/pipeline_lineage.hpp
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

namespace duckdb {
struct ChunkOffset;

class PipelineLineage {
public:
	virtual void AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) = 0;
	virtual idx_t GetChildChunkOffset(idx_t lineage_idx) = 0;
	virtual idx_t GetChunkOffset() = 0;
	virtual ~PipelineLineage() {};
};

class PipelineBreakerLineage : public PipelineLineage {
public:
	explicit PipelineBreakerLineage() {}

	void AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) override;
	idx_t GetChildChunkOffset(idx_t lineage_idx) override;
	idx_t GetChunkOffset() override;

private:
	shared_ptr<ChunkOffset> chunk_offset;
};

class PipelineJoinLineage : public PipelineLineage {
public:
	PipelineJoinLineage(const shared_ptr<PipelineLineage>& probe_child_node) :
	      probe_child_node(probe_child_node), next(false) {}

	void AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) override;
	idx_t GetChildChunkOffset(idx_t lineage_idx) override;
	idx_t GetChunkOffset() override;

	// Chunk Management
	void MarkChunkReturned();

private:
	shared_ptr<PipelineLineage> probe_child_node;
	shared_ptr<ChunkOffset> chunk_offset;

	bool next;
};

class PipelineScanLineage : public PipelineLineage {
public:
	PipelineScanLineage() {
		// TODO we init a chunk here to properly handle index joins that don't ever use table scans, this is a hack
		SetChunkId(0);
	}

	void AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) override;
	idx_t GetChildChunkOffset(idx_t lineage_idx) override;
	idx_t GetChunkOffset() override;

	void SetChunkId(idx_t id);

private:
	shared_ptr<ChunkOffset> chunk_offset;
	shared_ptr<ChunkOffset> filter_chunk_offset;
};

class PipelineSingleLineage : public PipelineLineage {
public:
	explicit PipelineSingleLineage(shared_ptr<PipelineLineage> child_node) : child_node(move(child_node)) {}

	void AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) override;
	idx_t GetChildChunkOffset(idx_t lineage_idx) override;
	idx_t GetChunkOffset() override;

private:
	shared_ptr<PipelineLineage> child_node;
	shared_ptr<ChunkOffset> chunk_offset;
};

struct ChunkOffset {
	idx_t offset;
	idx_t size;
};

} // namespace duckdb
#endif
