//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/art.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"

#include "duckdb/execution/index/art/art_key.hpp"
#include "duckdb/execution/index/art/leaf.hpp"
#include "duckdb/execution/index/art/node.hpp"
#include "duckdb/execution/index/art/node4.hpp"
#include "duckdb/execution/index/art/node16.hpp"
#include "duckdb/execution/index/art/node48.hpp"
#include "duckdb/execution/index/art/node256.hpp"

namespace duckdb {


struct LineageIndexScanState : public IndexScanState {
	LineageIndexScanState() : checked(false) {
	}

	Value values[2];
	ExpressionType expressions[2];
	bool checked;
	vector<row_t> result_ids;

};

enum class LineageIndexType{
	HASH_JOIN_PROBE,
	HASH_JOIN_SINK,
	FILTER,
	LIMIT,
	SCAN,
	GROUP_BY,
	AGGREGATION
};

class Lineage_Index : public Index {
public:
	Lineage_Index(const vector<column_t> &column_ids, const vector<unique_ptr<Expression>> &unbound_expressions,
	    string table, bool is_unique = false, bool is_primary = false);
	~Lineage_Index() override;

	//! Root of the tree
	unique_ptr<Node> tree;
	//! True if machine is little endian
	bool is_little_endian;

public:
	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for a single predicate
	unique_ptr<IndexScanState> InitializeScanSinglePredicate(Transaction &transaction, Value value,
	                                                         ExpressionType expressionType) override;

	//! Initialize a scan on the index with the given expression and column ids
	//! to fetch from the base table for two predicates
	unique_ptr<IndexScanState> InitializeScanTwoPredicates(Transaction &transaction, Value low_value,
	                                                       ExpressionType low_expression_type, Value high_value,
	                                                       ExpressionType high_expression_type) override;

	//! Perform a lookup on the index
	bool Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count,
	          vector<row_t> &result_ids) override;
	//! Append entries to the index
	bool Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Verify that data can be appended to the index
	void VerifyAppend(DataChunk &chunk) override;
	//! Delete entries in the index
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
	//! Insert data into the index.
	bool Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

	bool SearchEqual(LineageIndexScanState *state, idx_t max_count, vector<row_t> &result_ids);
	//! Search Equal used for Joins that do not need to fetch data
	void SearchEqualJoinNoFetch(Value &equal_value, idx_t &result_size);

private:
	DataChunk expression_result;

	string table_name;

	LineageIndexType cust_idx_type;

};

} // namespace duckdb
