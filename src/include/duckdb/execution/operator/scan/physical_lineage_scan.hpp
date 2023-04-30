#pragma once

#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/function/table/table_scan.hpp"

#include <duckdb/function/function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/table_filter.hpp>

namespace duckdb {

class PhysicalLineageScan : public PhysicalOperator {
public:
	explicit PhysicalLineageScan(ClientContext &context, shared_ptr<OperatorLineage> lineage_op, vector<LogicalType> types, TableFunction function, unique_ptr<FunctionData> bind_data,
	                             vector<column_t> column_ids, vector<string> names, unique_ptr<TableFilterSet> table_filters,
	                             idx_t estimated_cardinality);


	//! The table function
	TableFunction function;
	//! Bind data of the function
	unique_ptr<FunctionData> bind_data;
	//! The projected-out column ids
	vector<column_t> column_ids;
	//! The names of the columns
	vector<string> names;
	//! The table filters
	unique_ptr<TableFilterSet> table_filters;

	//! sub-operator index
	idx_t stage_idx;
	//! artifact log for this operator
	shared_ptr<OperatorLineage> lineage_op;
	//! column types for base table
	vector<LogicalType> base_table_types;
	//! column types for lineage data
	vector<LogicalType> lineage_table_types;
	//! entry to access tuples from base table
	TableCatalogEntry *base_tbl;

public:
	string GetName() const override;
	string ParamsToString() const override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) const override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};
} // namespace duckdb
