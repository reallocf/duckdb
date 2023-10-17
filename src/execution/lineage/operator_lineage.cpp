#ifdef LINEAGE
#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

LineageProcessStruct OperatorLineage::GetLineageAsChunk(const vector<LogicalType>& types, idx_t count_so_far,
                                              DataChunk &insert_chunk, idx_t size, int thread_id, idx_t data_idx, idx_t stage_idx) {
	return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
}

} // namespace duckdb
#endif
