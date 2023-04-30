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
	void PostProcess(PhysicalOperator *op);
	void InitOperatorPlan(PhysicalOperator *op, bool trace_lineage);
	idx_t CreateLineageTables(PhysicalOperator *op);
	void CreateQueryTable();
	void LogQuery(const string& input_query, idx_t lineage_size=0);
	static shared_ptr<PipelineLineage> GetPipelineLineageNodeForOp(PhysicalOperator *op, int thd_id=-1);
	static void CreateOperatorLineage(PhysicalOperator *op, int thd_id=-1, bool trace_lineage=true, bool should_index=true);

private:
	ClientContext &context;
	idx_t query_id = 0;
public:
	unordered_map<string, shared_ptr<OperatorLineage>> table_lineage_op;
	unordered_map<string, PhysicalOperator*> base_tables;
};


} // namespace duckdb
#endif
