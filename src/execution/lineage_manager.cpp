#ifdef LINEAGE
#include "duckdb/execution/lineage_context.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
class PhysicalOperator;

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 */
void ManageLineage::AnnotatePlan(PhysicalOperator *top_op) {
	idx_t c = 0;
	std::queue<PhysicalOperator*> op_queue;
	op_queue.push(top_op);
	while (!op_queue.empty()) {
		auto op = op_queue.front();
		op_queue.pop();
		op->id = c++;
		op->lineage_op = make_shared<LineageOp>(LineageOp());
#ifdef LINEAGE_DEBUG
        std::cout << op->GetName() << " " << op->id << std::endl;
#endif
		for (idx_t i = 0; i < op->children.size(); ++i){
			op_queue.push(op->children[i].get());
		}
	}
}

/*
 * Create table to store executed queries with their IDs
 * Table name: queries_list
 * Schema: (INT query_id, BLOB query)
 */
void ManageLineage::CreateQueryTable() {
	auto info = make_unique <CreateTableInfo>();
	info->schema = DEFAULT_SCHEMA;
	info->table = query_list_table_name;
	// This is recreated when a database is spun back up, so ignore
	info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	info->temporary = false;

	info->columns.emplace_back("query_id", LogicalType::INTEGER);
	info->columns.emplace_back("query", LogicalType::BLOB);

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(info));
	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateTable(context, bound_create_info.get());
}

/*
 * Persist executed query in queries_list table
 */
void ManageLineage::LogQuery(const string& input_query) {
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context)
	                             .GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, query_list_table_name);
  DataChunk insert_chunk;
  insert_chunk.Initialize(table->GetTypes());
  insert_chunk.SetCardinality(count);

  // query id
  Vector query_ids(Value::INTEGER(++query_id));

  // query value
  Vector payload(Value::BLOB(input_query));

  // populate chunk
  insert_chunk.data[0].Reference(query_ids);
  insert_chunk.data[1].Reference(payload);

  table->Persist(*table, context, insert_chunk);
}

vector<vector<ColumnDefinition>> GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN: {
		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source_table_columns));
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::BIGINT);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// build schema: [INTEGER in_index, BIGINT out_address] TODO convert from address to number?
		vector<ColumnDefinition> build_table_columns;
		build_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		build_table_columns.emplace_back("out_address", LogicalType::BIGINT);
		res.emplace_back(move(build_table_columns));
		// probe schema: [BIGINT lhs_address, INTEGER rhs_index, INTEGER out_index]
		vector<ColumnDefinition> probe_table_columns;
		probe_table_columns.emplace_back("lhs_address", LogicalType::BIGINT);
		probe_table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		probe_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(probe_table_columns));
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return res;
}

void ManageLineage::CreateLineageTables(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);

	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
		                    + op->GetName() + "_" + to_string(op->id) + "_" + to_string(i);

		auto info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = table_name;
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			info->columns.push_back(move(table_column_types[i][col_i]));
		}
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		TableCatalogEntry *table =
		    dynamic_cast<TableCatalogEntry *>(catalog.CreateTable(context, bound_create_info.get()));

		// Persist Data
		DataChunk insert_chunk;
		vector<LogicalType> types = table->GetTypes();
		insert_chunk.Initialize(types);
		LineageProcessStruct lps = op->lineage_op->Process(table->GetTypes(), 0, insert_chunk);
		while (lps.still_processing) {
			table->Persist(*table, context, insert_chunk);
			lps = op->lineage_op->Process(table->GetTypes(), lps.count_so_far, insert_chunk);
		}
		op->lineage_op->FinishedProcessing();
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(op->children[i].get());
	}
}

} // namespace duckdb
#endif
