//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/scan/physical_chunk_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! The PhysicalChunkCollectionScan scans a Chunk Collection
class PhysicalChunkScan : public PhysicalOperator {
public:
	PhysicalChunkScan(vector<LogicalType> types, PhysicalOperatorType op_type, idx_t estimated_cardinality, bool should_be_set = false)
	    : PhysicalOperator(op_type, move(types), estimated_cardinality), collection(nullptr), should_be_set(should_be_set) {
	}

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

public:
	// the chunk collection to scan
	ChunkCollection *collection;
	//! Owned chunk collection, if any
	unique_ptr<ChunkCollection> owned_collection;
	//! Whether or not we should assert a value in the chunk collection should be set
	bool should_be_set;
};

} // namespace duckdb
