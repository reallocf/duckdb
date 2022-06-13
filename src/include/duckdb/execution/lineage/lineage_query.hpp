//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_query.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/main/query_result.hpp"

namespace duckdb {

class LineageQuery {
public:
	unique_ptr<QueryResult> Run(
	    PhysicalOperator *op,
	    ClientContext &context,
	    const string& mode,
	    int lineage_id,
	    bool should_count
	);
};

} // namespace duckdb
#endif
