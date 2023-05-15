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

#ifndef QUERY_LIST_TABLE_NAME
#define QUERY_LIST_TABLE_NAME "queries_list"
#endif

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
	void CreateLineageTables(PhysicalOperator *op);

	//! Create queries_list table
	void CreateQueryTable();

	//! Add entries to queries_list
	void LogQuery(const string& input_query, string extra_meta="");

	static shared_ptr<PipelineLineage> GetPipelineLineageNodeForOp(PhysicalOperator *op, int thd_id=-1);

	void StoreQueryLineage(std::unique_ptr<PhysicalOperator> op, string query) {
		if (!trace_lineage) return;
		CreateLineageTables(op.get());
		LogQuery(query);
		query_to_plan[query] = move(op);
	}

	void SetCurrentLineageOp(shared_ptr<OperatorLineage> lop) {
		current_lop = lop;
	}

	shared_ptr<OperatorLineage> GetCurrentLineageOp() {
		return current_lop;
	}

private:
	ClientContext &context;

	//! id for current executed query
	idx_t query_id = 0;

	//! cached operator lineage to be accessed from function calls that don't have access to operator members
	shared_ptr<OperatorLineage> current_lop;

public:
	//! Whether or not lineage is currently being captured
	bool trace_lineage = false;

	//! Persist intermediate chunks in-memory
	bool persist_intermediate = false;

	//! map between lineage relational table name and its in-mem lineage
	unordered_map<string, shared_ptr<OperatorLineage>> table_lineage_op;

	//! in_memory storage of physical query plan per query
	std::unordered_map<string, std::unique_ptr<PhysicalOperator>> query_to_plan;
};


} // namespace duckdb
#endif
