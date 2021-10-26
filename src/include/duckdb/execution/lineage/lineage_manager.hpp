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

	void AnnotatePlan(PhysicalOperator *op);
	void CreateLineageTables(PhysicalOperator *op);
	void CreateQueryTable();
	void LogQuery(const string& input_query);

private:
	ClientContext &context;
	idx_t query_id = 0;
};


} // namespace duckdb
#endif
