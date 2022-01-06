//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_data.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/common.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"

#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"

namespace duckdb {

struct query {
	string base_table;
	string sink;
	string scan;
	string from;
	string in_select;
	string out_select;
	string condition;
};

struct query GetEndToEndQuery(PhysicalOperator *op, idx_t qid);

} // namespace duckdb
#endif
