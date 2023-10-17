#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::BuildIndexes() {
}

// TableScanLineage
//

void TableScanLineage::BuildIndexes() {
  // Binary Search Index
  auto size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    if (lineage[i].count == 0) continue;
    count_so_far += lineage[i].count;
    index.push_back(count_so_far);
  }
}

// HALineage
//

void HALineage::BuildIndexes() {
  // build side
	auto size = sink_log.size();
  idx_t count_so_far = 0;
  for (auto i=0; i < size; i++) {
    if (sink_log[i].branch == 0) {
      auto lsn = sink_log[i].lsn;
      idx_t res_count = addchunk_log[lsn].count;
      auto payload = addchunk_log[lsn].addchunk_lineage.get();
      for (idx_t j=0; j < res_count; ++j) {
        // TODO: add child pointer
        hash_map_agg[(idx_t)payload[j]].push_back(j + count_so_far);
      }
      count_so_far += res_count;
    }
  }
  std::cout << " hash agg index side: " << hash_map_agg.size() << std::endl;

  // scan side
  // Binary Search Index
  size = scan_log.size();
  index.reserve(size);
  count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    count_so_far += scan_log[i].count;
    index.push_back(count_so_far);
  }
  std::cout << "end" << std::endl;
}

// PHALineage
//

void PHALineage::BuildIndexes() {
	auto size = build_lineage.size();
  idx_t count_so_far = 0;
  for (auto i=0; i < size; i++) {
    idx_t res_count = build_lineage[i].size();
    auto payload = build_lineage[i];
    for (idx_t j=0; j < res_count; ++j) {
      // TODO: add child pointer
      hash_map_agg[(idx_t)payload[j]].push_back(j + count_so_far);
    }
    count_so_far += res_count;
  }
  std::cout << "Perfect hash agg index side: " << hash_map_agg.size() << std::endl;
  
  // scan side
  // Binary Search Index
  size = scan_lineage.size();
  index.reserve(size);
  count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    count_so_far += scan_lineage[i].count;
    index.push_back(count_so_far);
  }
}


// MergeLineage
//

void MergeLineage::BuildIndexes() {
  // Binary Search Index
  idx_t size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (idx_t i=0; i < size; ++i) {
    count_so_far += lineage[i].count;
    index.push_back(count_so_far);
  }
}


// BNLJLineage
//

void BNLJLineage::BuildIndexes() {
  // Binary Search Index
  idx_t size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (idx_t i=0; i < size; ++i) {
    count_so_far += lineage[i].count;
    index.push_back(count_so_far);
  }
}


// NLJLineage
//

void NLJLineage::BuildIndexes() {
  // Binary Search Index
  idx_t size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (idx_t i=0; i < size; ++i) {
    count_so_far += lineage[i].count;
    index.push_back(count_so_far);
  }
}


// CrossLineage
//

void CrossLineage::BuildIndexes() {
  // Binary Search Index
  idx_t size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (idx_t i=0; i < size; ++i) {
    count_so_far += lineage[i].left_chunk;
    index.push_back(count_so_far);
  }
}

// IndexJoinLineage
//

void IndexJoinLineage::BuildIndexes() {
}


// HashJoinLineage
//

void HashJoinLineage::BuildIndexes() {
  // build
			idx_t size = lineage_build.size();
      start_base = 0;
      last_base = 0;
      idx_t count_so_far = 0;
      offset = 0;
      if (size > 0) {
        auto payload = (uint64_t*)(lineage_build[0].scatter.get());
        idx_t res_count = lineage_build[0].count;
        start_base = payload[0];
        last_base = payload[res_count - 1];
        hm_range.emplace_back(start_base, last_base);
        hash_chunk_count.push_back(0);
        if (offset == 0 && res_count > 1) {
          offset = payload[1] - payload[0];
        }
        count_so_far += res_count;
      }

      for (auto i=1; i < size; ++i) {
        // build hash table with range -> acc
        // if x in range -> then use range.start and adjust the value using acc
        auto payload = (uint64_t*)(lineage_build[i].scatter.get());
        idx_t res_count = lineage_build[i].count;
        if (offset == 0) offset = payload[res_count - 1] - start_base;
        auto diff = (payload[res_count - 1] - start_base) / offset;
        if (diff + 1 !=  count_so_far + res_count - hash_chunk_count.back()) {
          // update the range and log the old one
          // range -> count
          // if value fall in this range, then remove the start / offset
          for (idx_t j = 0; j < res_count; ++j) {
            auto f = ((payload[j] - start_base) / offset);
            auto s = count_so_far + j - hash_chunk_count.back();
            if ( f !=  s) {
              if (j > 1) {
                hm_range.back().second = payload[j - 1]; // the previous one
              }
              hash_chunk_count.push_back(count_so_far + j);
              start_base = payload[j];
              last_base = payload[res_count - 1];
              hm_range.emplace_back(start_base, last_base);
              break;
            }
          }
        } else {
          hm_range.back().second = payload[res_count - 1];
        }
        count_so_far += res_count;
      }

      // scan
      // Binary Search Index
			size = output_index.size();
			index.reserve(size);
      count_so_far = 0;
      // O(number of chunks)
      for (auto i=0; i < size; ++i) {
        auto lsn = output_index[i];
			  count_so_far += lineage_binary[lsn].count;
				index.push_back(count_so_far);
      }
}


// OrderByLineage
//

void OrderByLineage::BuildIndexes() {
  // Binary Search Index
  auto size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    count_so_far += lineage[i].size();
    index.push_back(count_so_far);
  }
}

// FilterLineage
//

void FilterLineage::BuildIndexes() {
  // Binary Search Index
  auto size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    count_so_far += lineage[i].count;
    index.push_back(count_so_far);
  }
}

// LimitLineage
//

void LimitLineage::BuildIndexes() {
  // Binary Search Index
  auto size = lineage.size();
  index.reserve(size);
  idx_t count_so_far = 0;
  // O(number of chunks)
  for (auto i=0; i < size; ++i) {
    count_so_far += (lineage[i].end - lineage[i].start);
    index.push_back(count_so_far);
  }
}


} // namespace duckdb
#endif
