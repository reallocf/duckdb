//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_manager.hpp
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
#include "operator_lineage.hpp"

#include <iostream>
#include <utility>

namespace duckdb {
class ClientContext;
class PhysicalOperator;


class LineageManager {
public:
	explicit LineageManager(ClientContext &context) : context(context) {};
	vector<vector<ColumnDefinition>> GetTableColumnTypes(PhysicalOperator *op);

	//! 1. call PlanAnnotator: For each operator in the plan, give it an ID. If there are
	//! two operators with the same type, give them a unique ID starting
	//! from the zero and incrementing it for the lowest levels of the tree
	//! 2.  call CreateOperatorLineage to allocate lineage_op for main thread (id=-1)
	void InitOperatorPlan(PhysicalOperator *op);

	static void CreateOperatorLineage(PhysicalOperator *op, int thd_id=-1, bool trace_lineage=true, bool should_index=true);

	//! Create empty lineage tables for each operator
	void CreateLineageTables(PhysicalOperator *op, idx_t query_id);

	void StoreQueryLineage(std::unique_ptr<PhysicalOperator> op, string query);

private:
	ClientContext &context;

public:
	//! Whether or not lineage is currently being captured
	bool trace_lineage = false;

	//! Persist intermediate chunks in-memory
	bool persist_intermediate = false;

	//! map between lineage relational table name and its in-mem lineage
	unordered_map<string, std::unordered_map<int, shared_ptr<OperatorLineage>>> table_lineage_op;

	//! in_memory storage of physical query plan per query
	std::unordered_map<idx_t, std::unique_ptr<PhysicalOperator>> queryid_to_plan;

	//! map between query_id and query string
	//! id for current executed query query_to_id.size()
	vector<string> query_to_id;
};


} // namespace duckdb
#endif
