#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"

namespace duckdb {

class PhysicalChunkScanState : public PhysicalOperatorState {
public:
	explicit PhysicalChunkScanState(PhysicalOperator &op) : PhysicalOperatorState(op, nullptr), chunk_index(0) {
	}

	//! The current position in the scan
	idx_t chunk_index;
};

void PhysicalChunkScan::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                         PhysicalOperatorState *state_p) const {
	std::cout << "Woo1" << std::endl;
	auto state = (PhysicalChunkScanState *)state_p;
	D_ASSERT(!should_be_set || collection->Count() > 0);
	D_ASSERT(collection);
	std::cout << "Woo2" << std::endl;
	if (collection->Count() == 0) {
		return;
	}
	std::cout << "Woo3" << std::endl;
	D_ASSERT(chunk.GetTypes() == collection->Types());
	if (state->chunk_index >= collection->ChunkCount()) {
		return;
	}
	std::cout << "Woo4: " << state->chunk_index << " " << collection->Count() << std::endl;
	auto &collection_chunk = collection->GetChunk(state->chunk_index);
	std::cout << "Woo5" << std::endl;
	chunk.Reference(collection_chunk);
	std::cout << "Woo6" << std::endl;
	state->chunk_index++;
}

unique_ptr<PhysicalOperatorState> PhysicalChunkScan::GetOperatorState() {
	return make_unique<PhysicalChunkScanState>(*this);
}

} // namespace duckdb
