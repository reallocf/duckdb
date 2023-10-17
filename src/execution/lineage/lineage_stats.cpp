#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

idx_t OperatorLineage::Size() {
	idx_t size = 0;
	return size;
}

idx_t OperatorLineage::ChunksCount() {
  idx_t count = 0;
  return count;
}

idx_t OperatorLineage::Count() {
  idx_t count = 0;
  return count;
}

LineageProcessStruct::LineageProcessStruct(idx_t i, idx_t i1, idx_t i2, idx_t i3, bool b) {
	count_so_far = i;
	size_so_far = i1;
	data_idx = i2;
	finished_idx = i3;
	still_processing = b;
}

// TableScanLineage
//
idx_t TableScanLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
    if (lineage_data.sel != nullptr)
		  size += lineage_data.count * sizeof(sel_t);
    size += sizeof(scan_artifact);
	}
	return size;
}

idx_t TableScanLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += lineage_data.count;
	}
	return count;
}

idx_t TableScanLineage::ChunksCount() {
  return lineage.size();
}

// HALineage
//
idx_t HALineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : addchunk_log) {
		size += lineage_data.count * sizeof(data_t);
    size += sizeof(hg_artifact);
	}
	
  for (const auto& lineage_data : sink_log) {
    size += sizeof(sink_artifact);
	}
	
  for (const auto& lineage_data : flushmove_log) {
		size += 2*(lineage_data.count * sizeof(data_t));
    size += sizeof(flushmove_artifact);
	}
	
  for (const auto& lineage_data : partition_log) {
    size += sizeof(partition_artifact);
	}

  // radix_log
  for (const auto& lineage_data : radix_log) {
    for (const auto& r: lineage_data) {
      size += r.sel_size * sizeof(sel_t);
      size += sizeof(radix_artifact);
    }
	}

  // combine_log
  for (const auto& lineage_data : combine_log) {
    size += lineage_data.size() * sizeof(void*);
	}

  // finalize_log
  for (const auto& lineage_data : finalize_log) {
    if (lineage_data.combine)
      size += lineage_data.combine->size() * sizeof(void*);
    size += sizeof(finalize_artifact);
	}

  // scan_log
	for (const auto& lineage_data : scan_log) {
		size += lineage_data.count * sizeof(data_t);
    size += sizeof(hg_artifact);
	}
	
	return size;
}

idx_t HALineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : addchunk_log) {
		count += lineage_data.count;
	}
  // scan_log
	return count;
}
idx_t HALineage::ChunksCount() {
  return sink_log.size() + scan_log.size();
}

// PHALineage
//
idx_t PHALineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : build_lineage) {
		size += lineage_data.size() * sizeof(uint32_t);
	}
	
	for (const auto& lineage_data : scan_lineage) {
		size += lineage_data.count * sizeof(uint32_t);
    size += sizeof(pha_scan_artifact);
	}
	
	return size;
}

idx_t PHALineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : build_lineage) {
		count += lineage_data.size();
	}
	
	for (const auto& lineage_data : scan_lineage) {
		count += lineage_data.count;
	}
	
	return count;
}


idx_t PHALineage::ChunksCount() {
  return build_lineage.size() + scan_lineage.size();
}

// MergeLineage
//
idx_t MergeLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
		size += 2* (lineage_data.count * sizeof(sel_t));
    size += sizeof(merge_artifact);
	}
	return size;
}

idx_t MergeLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += 2* (lineage_data.count);
	}
	return count;
}
idx_t MergeLineage::ChunksCount() {
  return lineage.size();
}

// BNLJLineage
//
idx_t BNLJLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
		size += (lineage_data.count * sizeof(sel_t));
    size += sizeof(bnlj_artifact);
	}
	return size;
}

idx_t BNLJLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += lineage_data.count;
	}
	return count;
}
idx_t BNLJLineage::ChunksCount() {
  return lineage.size();
}

// NLJLineage
//
idx_t NLJLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
		size += 2* (lineage_data.count * sizeof(sel_t));
    size += sizeof(nlj_artifact);
	}
	return size;
}

idx_t NLJLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += 2* (lineage_data.count);
	}
	return count;
}

idx_t NLJLineage::ChunksCount() {
  return lineage.size();
}

// CrossLineage
//
idx_t CrossLineage::Size() {
	idx_t size = 0;
  size = lineage.size() * sizeof(cross_artifact);
	return size;
}

idx_t CrossLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += lineage_data.left_chunk;
	}
	return count;
}

idx_t CrossLineage::ChunksCount() {
  return lineage.size();
}

// IndexJoinLineage
//
idx_t IndexJoinLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
		size += lineage_data.count * sizeof(row_t);
		size += lineage_data.count * sizeof(sel_t);
    size += sizeof(IJ_artifact);
	}
	return size;
}

idx_t IndexJoinLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += lineage_data.count;
	}
	return 2*count;
}

idx_t IndexJoinLineage::ChunksCount() {
  return lineage.size();
}

// HashJoinLineage
//
idx_t HashJoinLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage_build) {
		size += lineage_data.count * sizeof(data_t);
    size += sizeof(hj_build_artifact);
	}
	
  for (const auto& lineage_data : lineage_binary) {
		size += lineage_data.count * sizeof(sel_t);
		size += lineage_data.count * sizeof(uintptr_t);
    size += sizeof(hj_probe_artifact);
	}

  size += output_index.size() * sizeof(void*);
	return size;
}

idx_t HashJoinLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage_build) {
		count += lineage_data.count;
	}
	
  for (const auto& lineage_data : lineage_binary) {
		count += 2* lineage_data.count;
	}

	return count;
}

idx_t HashJoinLineage::ChunksCount() {
  return lineage_binary.size() + lineage_build.size();
}

// OrderByLineage
//
idx_t OrderByLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
		size += lineage_data.size() * sizeof(idx_t);
	}
	return size;
}

idx_t OrderByLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {
		count += lineage_data.size();
	}
	return count;
}

idx_t OrderByLineage::ChunksCount() {
  return lineage.size();
}

// FilterLineage
//
idx_t FilterLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
    if (lineage_data.sel != nullptr)
		  size += lineage_data.count * sizeof(sel_t);
    size += sizeof(filter_artifact);
	}
	return size;
}

idx_t FilterLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {  
    count += lineage_data.count;
	}
	return count;
}

idx_t FilterLineage::ChunksCount() {
  return lineage.size();
}

// LimitLineage
//
idx_t LimitLineage::Size() {
	idx_t size = 0;
	for (const auto& lineage_data : lineage) {
    size += sizeof(limit_artifact);
	}
	return size;
}

idx_t LimitLineage::Count() {
	idx_t count = 0;
	for (const auto& lineage_data : lineage) {  
    count += (lineage_data.end - lineage_data.start);
	}
	return count;
}

idx_t LimitLineage::ChunksCount() {
  return lineage.size();
}


} // namespace duckdb
#endif
