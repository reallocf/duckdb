#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;
class PhysicalTableScan;

// Get the column types for this operator
// Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> LineageManager::GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::ORDER_BY: {
		// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source_table_columns));
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, BIGINT out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::BIGINT);
		sink_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink_table_columns));
		// source schema: [BIGINT in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::BIGINT);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source_table_columns));
		// combine schema: [BIGINT in_index, INTEGER out_index]
		vector<ColumnDefinition> combine_table_columns;
		combine_table_columns.emplace_back("in_index", LogicalType::BIGINT);
		combine_table_columns.emplace_back("out_index", LogicalType::BIGINT);
		combine_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(combine_table_columns));
		break;
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// sink: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);
		sink.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(sink));
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::BIGINT);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// build schema: [INTEGER in_index, BIGINT out_address] TODO convert from address to number?
		vector<ColumnDefinition> build_table_columns;
		build_table_columns.emplace_back("out_address", LogicalType::BIGINT);
		build_table_columns.emplace_back("in_index", LogicalType::BIGINT);
		build_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(build_table_columns));
		// probe schema: [BIGINT lhs_address, INTEGER rhs_index, INTEGER out_index]
		vector<ColumnDefinition> probe_table_columns;
		probe_table_columns.emplace_back("lhs_address", LogicalType::BIGINT);
		probe_table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		probe_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		probe_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(probe_table_columns));
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return res;
}


// Create the table for this operator and fill it with lineage
// Return's total lineage size in bytes
idx_t LineageManager::CreateLineageTables(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
	idx_t total_size = 0;
	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
		                    + op->GetName() + "_" + to_string(i);
		// Create Table
		auto info = make_unique<CreateTableInfo>(DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			info->columns.push_back(move(table_column_types[i][col_i]));
		}
		if (op->type == PhysicalOperatorType::TABLE_SCAN) {
			base_tables[table_name] = op;
			auto &phy_tbl_scan = (PhysicalTableScan &)*op;
			auto &bind_tbl = (TableScanBindData &)*phy_tbl_scan.bind_data;
			for (idx_t i=0; i < bind_tbl.table->columns.size(); ++i) {
				info->columns.push_back(bind_tbl.table->columns[i].Copy());
			}

		}

		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());
		table_lineage_op[table_name] = op->lineage_op.at(-1);
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		total_size += CreateLineageTables( dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		total_size += CreateLineageTables( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get());
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		total_size += CreateLineageTables(op->children[i].get());
	}
	return total_size;
}
/*
 * Create table to store executed queries with their IDs
 * Table name: queries_list
 * Schema: (INT query_id, varchar query, varchar extra)
 */
void LineageManager::CreateQueryTable() {
	auto info = make_unique <CreateTableInfo>(DEFAULT_SCHEMA, QUERY_LIST_TABLE_NAME);
	// This is recreated when a database is spun back up, so ignore
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;

	info->columns.emplace_back("query_id", LogicalType::INTEGER);
	info->columns.emplace_back("query", LogicalType::VARCHAR);
	info->columns.emplace_back("extra", LogicalType::VARCHAR);

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(info));
	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateTable(context, bound_create_info.get());
}

/*
 * Persist executed query in queries_list table
 */
void LineageManager::LogQuery(const string& input_query, idx_t lineage_size) {
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context)
	                             .GetEntry<TableCatalogEntry>(context,DEFAULT_SCHEMA, QUERY_LIST_TABLE_NAME);
  DataChunk insert_chunk;
  insert_chunk.Initialize(table->GetTypes());
  insert_chunk.SetCardinality(count);

  // query id
  Vector query_ids(Value::INTEGER(query_id++));

  // query value
  Vector payload(input_query);

  // extra info, e.g size in bytes
  string extra_str = to_string(lineage_size);
  Vector extra(extra_str);

  // populate chunk
  insert_chunk.data[0].Reference(query_ids);
  insert_chunk.data[1].Reference(payload);
  insert_chunk.data[2].Reference(extra);

  table->Persist(*table, context, insert_chunk);
}

} // namespace duckdb
#endif