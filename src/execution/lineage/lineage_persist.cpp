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
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;
class PhysicalTableScan;
class PhysicalProjection;

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> LineageManager::GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::ORDER_BY: {
		// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> source;
		source.emplace_back("in_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);

		if (op->type == PhysicalOperatorType::HASH_GROUP_BY)
			sink.emplace_back("out_index", LogicalType::BIGINT);
		else
			sink.emplace_back("out_index", LogicalType::INTEGER);

		sink.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink));

		// source schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> source;

		if (op->type == PhysicalOperatorType::HASH_GROUP_BY)
			source.emplace_back("in_index", LogicalType::BIGINT);
		else
			source.emplace_back("in_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// sink: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> sink;
		sink.emplace_back("in_index", LogicalType::INTEGER);

		if (op->type == PhysicalOperatorType::HASH_JOIN) {
			sink.emplace_back("out_index", LogicalType::BIGINT);
		} else {
			sink.emplace_back("out_index", LogicalType::INTEGER);
		}

		sink.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink));

		// schema: [INTEGER lhs_index, INTEGER|BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> source;
		source.emplace_back("lhs_index", LogicalType::INTEGER);

		if (op->type == PhysicalOperatorType::INDEX_JOIN || op->type == PhysicalOperatorType::HASH_JOIN)
			source.emplace_back("rhs_index", LogicalType::BIGINT);
		else
			source.emplace_back("rhs_index", LogicalType::INTEGER);

		source.emplace_back("out_index", LogicalType::INTEGER);
		source.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return res;
}


//! Construct empty tables for operators lineage to be accessed using Lineage Scan
void LineageManager::CreateLineageTables(PhysicalOperator *op, idx_t query_id) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
		                    + op->GetName() + "_" + to_string(i);
		// Create Table
		auto info = make_unique<CreateTableInfo>(DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			info->columns.push_back(move(table_column_types[i][col_i]));
		}

		table_lineage_op[table_name] = op->lineage_op.at(-1);

		// add column_stats, cardinality
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		TableCatalogEntry* table = (TableCatalogEntry*)catalog.CreateTable(context, bound_create_info.get());
		table->storage->info->cardinality = 100;
		table->storage->UpdateStats();
	}

	// persist intermediate values
	if (persist_intermediate) {
		vector<ColumnDefinition> table;
		for (idx_t col_i = 0; col_i < op->types.size(); col_i++) {
			table.emplace_back("col_" + to_string(col_i), op->types[col_i]);
		}

		string table_name = "LINEAGE_" + to_string(query_id) + "_"  + op->GetName() + "_100";
		auto info = make_unique<CreateTableInfo>(DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table.size(); col_i++) {
			info->columns.push_back(move(table[col_i]));
		}
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());
		table_lineage_op[table_name] = op->lineage_op.at(-1);
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		CreateLineageTables( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), query_id);
		CreateLineageTables( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), query_id);
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(op->children[i].get(), query_id);
	}
}

} // namespace duckdb
#endif
