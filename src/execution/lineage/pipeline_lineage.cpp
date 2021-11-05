#ifdef LINEAGE
#include "duckdb/execution/lineage/pipeline_lineage.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

// PipelineBreakerLineage

void PipelineBreakerLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_SOURCE) {
		if (chunk_offset == nullptr) {
			chunk_offset = make_shared<ChunkOffset>();
			chunk_offset->offset = 0;
		} else {
			// Increase offset if not first iteration
			chunk_offset->offset = chunk_offset->offset + chunk_offset->size;
		}
		chunk_offset->size = chunk_size;
	} else {
		// We don't care about the sink's offset since it's never used
	}
}

idx_t PipelineBreakerLineage::GetChildChunkOffset(idx_t lineage_idx) {
	// We don't care about the child chunk offset for pipeline breaker
	return 0;
}

idx_t PipelineBreakerLineage::GetChunkOffset() {
	if (chunk_offset != nullptr) {
		return chunk_offset->offset;
	} else {
		// Lineage unimplemented
		return 0;
	}
}

// PipelineJoinLineage

void PipelineJoinLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_PROBE) {
		if (chunk_offset == nullptr) {
			// First iteration
			chunk_offset = make_shared<ChunkOffset>();
			chunk_offset->offset = 0;
			chunk_offset->size = chunk_size;
		} else if (next) {
			// If we've just pushed to the parent operator, adjust offset based on the current chunk
			chunk_offset->offset = chunk_offset->offset + chunk_offset->size;
			chunk_offset->size = chunk_size;
			next = false;
		} else {
			// If we haven't pushed to the parent operator, offset remains the same (chunk merge)
			chunk_offset->offset = chunk_offset->offset;
			chunk_offset->size = chunk_offset->size + chunk_size;
		}
	} else {
		// We don't care about the build's offset since it's never used
	}
}

idx_t PipelineJoinLineage::GetChildChunkOffset(idx_t lineage_idx) {
	if (lineage_idx == LINEAGE_BUILD) {
		return build_child_node->GetChunkOffset();
	} else {
		return probe_child_node->GetChunkOffset();
	}
}

idx_t PipelineJoinLineage::GetChunkOffset() {
	if (chunk_offset != nullptr) {
		return chunk_offset->offset;
	} else {
		// Lineage unimplemented
		return 0;
	}
}

void PipelineJoinLineage::MarkChunkReturned() {
	next = true;
}

// PipelineScanLineage

void PipelineScanLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	if (filter_chunk_offset == nullptr) {
		filter_chunk_offset = make_shared<ChunkOffset>();
		filter_chunk_offset->offset = 0;
	} else {
		filter_chunk_offset->offset = filter_chunk_offset->offset + filter_chunk_offset->size;
	}
	filter_chunk_offset->size = chunk_size;
}

idx_t PipelineScanLineage::GetChildChunkOffset(idx_t lineage_idx) {
	// Child lineage for Scan is adjusting the pushed down filter based on what chunk we're in
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	return chunk_offset->offset;
}

idx_t PipelineScanLineage::GetChunkOffset() {
	if (chunk_offset == nullptr) {
		// Lineage unimplemented
		return 0;
	} if (filter_chunk_offset == nullptr) {
		// TODO is this right?
		return chunk_offset->offset;
	} else {
		return filter_chunk_offset->offset;
	}
}

void PipelineScanLineage::SetChunkId(idx_t id) {
	if (chunk_offset == nullptr) {
		chunk_offset = make_shared<ChunkOffset>();
	}
	chunk_offset->offset = id * STANDARD_VECTOR_SIZE;
	chunk_offset->size = STANDARD_VECTOR_SIZE;
}

// PipelineSingleLineage

void PipelineSingleLineage::AdjustChunkOffsets(idx_t chunk_size, idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	if (chunk_offset == nullptr) {
		chunk_offset = make_shared<ChunkOffset>();
		chunk_offset->offset = 0;
	} else {
		chunk_offset->offset = chunk_offset->offset + chunk_offset->size;
	}
	chunk_offset->size = chunk_size;
}

idx_t PipelineSingleLineage::GetChildChunkOffset(idx_t lineage_idx) {
	D_ASSERT(lineage_idx == LINEAGE_UNARY);
	return child_node->GetChunkOffset();
}

idx_t PipelineSingleLineage::GetChunkOffset() {
	if (chunk_offset != nullptr) {
		return chunk_offset->offset;
	} else {
		// Lineage unimplemented
		return 0;
	}
}

} // namespace duckdb
#endif
