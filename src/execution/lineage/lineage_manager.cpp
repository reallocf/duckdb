#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include <utility>

namespace duckdb {
class PhysicalDelimJoin;

shared_ptr<PipelineLineage> GetPipelineLineageNodeForOp(PhysicalOperator *op) {
	switch (op->type) {
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		return make_shared<PipelineScanLineage>();
	}
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER: {
		return make_shared<PipelineSingleLineage>(op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::ORDER_BY: {
		return make_shared<PipelineBreakerLineage>();
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::HASH_JOIN: {
		return make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		return make_shared<PipelineJoinLineage>(op->children[0]->lineage_op->GetPipelineLineage());
	}
	case PhysicalOperatorType::PROJECTION: {
		// Pass through to last operator
		return op->children[0]->lineage_op->GetPipelineLineage();
	}
	default:
		// Lineage unimplemented! TODO these :)
		return nullptr;
	}
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
idx_t PlanAnnotator(PhysicalOperator *op, idx_t counter, bool trace_lineage) {
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter, trace_lineage);
		counter = PlanAnnotator( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter, trace_lineage);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i)
			counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i], counter, trace_lineage);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter, trace_lineage);
	}
	op->id = counter;
	op->lineage_op = make_shared<OperatorLineage>(GetPipelineLineageNodeForOp(op), op->type);
	op->lineage_op->trace_lineage = trace_lineage;
	return counter + 1;
}

struct query {
	string base_table;
	string sink;
	string scan;
	string from;
	string in_select;
	string out_select;
	string condition;
};

struct query join_caluse(string table_name, query first_child, query second_child, idx_t child_1_id, idx_t child_2_id, JoinType join_type) {
	query Q;
	Q.scan = table_name+"1";
	Q.sink =  table_name+"0";
	Q.out_select = Q.scan+".out_index";

	switch (join_type) {
	case JoinType::ANTI: {
		Q.from = Q.scan;
		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select = first_child.in_select;
			Q.condition += " and " + Q.scan + ".rhs_index=" + first_child.scan + ".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select += Q.scan + ".rhs_index as " + first_child.base_table;
		} else {
			Q.in_select = Q.scan + ".rhs_index as " + Q.scan + "_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition +=  first_child.condition;
		break;
	}
	case JoinType::SEMI:
	case JoinType::INNER: {
		Q.from = Q.scan + ", " + Q.sink;
		Q.condition = Q.sink + ".out_address=" + Q.scan + ".lhs_address";


		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select = first_child.in_select;
			Q.condition += " and " + Q.scan + ".rhs_index=" + first_child.scan + ".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select += Q.scan + ".rhs_index as " + first_child.base_table;
		} else {
			Q.in_select = Q.scan + ".rhs_index as " + Q.scan + "_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition += " and " + first_child.condition;

		if (!second_child.from.empty()) {
			Q.from += ", " + second_child.from;
			Q.in_select += ", " + second_child.in_select;
			Q.condition += " and " + Q.sink + ".in_index=" + second_child.scan + ".out_index";
		} else if (!second_child.base_table.empty()) {
			Q.in_select += ", " + Q.sink + ".in_index as " + second_child.base_table;
		} else {
			Q.in_select += ", " + Q.sink + ".in_index as " + Q.sink + "_rowid";
		}
		if (!second_child.condition.empty())
			Q.condition += " and " + second_child.condition;


		break;
	}
	case JoinType::RIGHT: {
		Q.from = Q.sink + " left join "+ Q.scan + " on (";
		Q.from += Q.sink+".out_address="+Q.scan+".lhs_address)";


		if (!first_child.from.empty()) {
			Q.from += " left join ";
			Q.from += "(select "+first_child.in_select+", "
			          +first_child.out_select+" as out_index from "
			          + first_child.from + " where " +first_child.condition
			          + ") as temp_"+to_string(child_2_id);
			Q.from += " on (";
			Q.from +=  Q.scan + ".rhs_index=temp_"+to_string(child_2_id)+".out_index)";
			Q.in_select =  "temp_"+to_string(child_2_id)+".*";
		} else if (!first_child.base_table.empty()) {
			Q.in_select = Q.scan + ".rhs_index as " + first_child.base_table;
		} else {
			Q.in_select = Q.scan + ".rhs_index as " + Q.scan + "_rowid";
		}

		if (!second_child.from.empty()) {
			Q.from += ", " + second_child.from;
			Q.in_select += ", " + second_child.in_select;
			Q.condition += " and " + Q.sink + ".in_index=" + second_child.scan + ".out_index";
		} else if (!second_child.base_table.empty()) {
			Q.in_select += ", " + Q.sink + ".in_index as " + second_child.base_table;
		} else {
			Q.in_select += ", " + Q.sink + ".in_index as " + Q.sink + "_rowid";
		}
		if (!second_child.condition.empty())
			Q.condition += " and " + second_child.condition;
		break;
	}
	case JoinType::OUTER: {
		break;
	}
	case JoinType::MARK:
	case JoinType::SINGLE:
	case JoinType::LEFT: {
		Q.from = Q.scan + " left join "+ Q.sink + " on (";
		Q.from += Q.sink+".out_address="+Q.scan+".lhs_address)";

		if (!second_child.from.empty()) {
			Q.from += " left join " + second_child.from + " on (";
			Q.from +=  Q.sink + ".in_index=" + second_child.scan + ".out_index)";
			Q.in_select +=  second_child.in_select;
		} else if (!second_child.base_table.empty()) {
			Q.in_select +=  Q.sink + ".in_index as " + second_child.base_table;
		} else {
			Q.in_select +=  Q.sink + ".in_index as " + Q.sink + "_rowid";
		}

		if (!second_child.condition.empty())
			Q.condition += second_child.condition;

		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select += ", " + first_child.in_select;
			if (!Q.condition.empty())
				Q.condition += " and ";
			Q.condition +=  Q.scan + ".rhs_index=" + first_child.scan + ".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select += ", " + Q.scan + ".rhs_index as " + first_child.base_table;
		} else {
			Q.in_select = ", "  + Q.scan + ".rhs_index as " + Q.scan + "_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition += " and " + first_child.condition;
		break;
	}
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
	return Q;
}
// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
struct query GetEndToEndQuery(PhysicalOperator *op, idx_t qid) {

	// Example: LINEAGE_1_HASH_JOIN_3_0
	string table_name = "LINEAGE_" + to_string(qid) + "_"
	                    + op->GetName() + "_"; // + to_string(i);
	query Q;
	switch (op->type) {
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		Q.scan = table_name + "1";
		Q.sink = table_name + "0";
		Q.from = Q.scan;
		Q.out_select = Q.scan + ".out_index";
		Q.condition = Q.scan+".in_index=="+Q.sink+".out_index";
		query child;
		if (op->children.size() > 0)
			child = GetEndToEndQuery(op->children[0].get(), qid);
		if (!child.from.empty()) {
			Q.in_select = child.in_select;
			Q.from += ", "+child.from;
			Q.condition += " and "+child.scan+".out_index="+Q.scan+".in_index";
			if (!child.condition.empty())
				Q.condition += " and " + child.condition;
		} else if (!child.base_table.empty()) {
			Q.in_select = Q.scan+".in_index as "+child.base_table;
		} else {
			Q.in_select = Q.scan+".in_index as "+ Q.scan+"_rowid";
		}

		break;
	}
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::ORDER_BY: {
		Q.scan = table_name + "0";
		Q.from = Q.scan;
		Q.out_select = Q.scan + ".out_index";

		query child;
		if (op->children.size() > 0)
			child = GetEndToEndQuery(op->children[0].get(), qid);
		if (!child.from.empty()) {
			Q.in_select = child.in_select;
			Q.from += ", "+child.from;
			Q.condition += child.scan+".out_index="+Q.scan+".in_index";
			if (!child.condition.empty())
				Q.condition += " and " + child.condition;
		} else if (!child.base_table.empty()) {
			Q.in_select = Q.scan+".in_index as "+child.base_table;
		} else {
			Q.in_select = Q.scan+".in_index as "+ Q.scan+"_rowid";
		}

		break;
	}
	case PhysicalOperatorType::DELIM_SCAN: {
		Q.base_table = "delimscan_rowid_opid_"+ to_string(op->id);
		break;
	}
	case PhysicalOperatorType::CHUNK_SCAN: {
		Q.base_table = "chunkscan_rowid_opid_"+ to_string(op->id);
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		string name = dynamic_cast<PhysicalTableScan *>(op)->function.to_string(dynamic_cast<PhysicalTableScan *>(op)->bind_data.get());
		Q.base_table = name+"_rowid_opid_"+ to_string(op->id);
		if (!dynamic_cast<PhysicalTableScan *>(op)->table_filters)
			return Q;

		Q.scan = table_name + "0";
		Q.from = Q.scan;
		Q.in_select = Q.scan + ".in_index as "+Q.base_table;
		Q.out_select = Q.scan + ".out_index";
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		Q.scan = table_name+"0";
		Q.from = Q.scan;
		Q.out_select = Q.scan+".out_index";

		query first_child = GetEndToEndQuery(op->children[0].get(), qid);
		query second_child = GetEndToEndQuery(op->children[1].get(), qid);
		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select = first_child.in_select;
			Q.condition += Q.scan+".rhs_index="+first_child.scan+".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select += Q.scan+".rhs_index as "+first_child.base_table;
		} else {
			Q.in_select = Q.scan+".rhs_index as "+Q.scan+"_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition += " and " + first_child.condition;

		if (!second_child.from.empty()) {
			Q.from += ", " + second_child.from;
			Q.in_select += ", " + second_child.in_select;
			if (!Q.condition.empty())
				Q.condition += " and ";
			Q.condition += Q.scan+".lhs_index="+second_child.scan+".out_index";
		} else if (!second_child.base_table.empty()) {
			Q.in_select += ", "+Q.scan+".lhs_index as "+second_child.base_table;
		} else {
			Q.in_select += ", "+Q.scan+".lhs_index as "+Q.scan+"_rowid";
		}
		if (!second_child.condition.empty())
			Q.condition += " and " + second_child.condition;
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// 0->left, 1->right, but since we flipped it it is the opposite for now
		query first_child = GetEndToEndQuery(op->children[0].get(), qid);
		query second_child = GetEndToEndQuery(op->children[1].get(), qid);
		Q = join_caluse(table_name,first_child, second_child, op->children[0]->id, op->children[1]->id, dynamic_cast<PhysicalJoin *>(op)->join_type);
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		Q = GetEndToEndQuery(op->children[0].get(), qid);
		Q.scan = "temp_opio"+ to_string(op->id);
		Q.from = "(SELECT "+ Q.in_select+", 0 as out_index from "+Q.from;
		if (!Q.condition.empty())
			Q.from +=" where "+Q.condition;
		Q.from += +") as "+ Q.scan;
		Q.in_select = Q.scan+".*";
		Q.out_select = "0 as out_index";
		Q.condition = "";
		break;
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		std::cout <<dynamic_cast<PhysicalDelimJoin *>(op)->join->ToString() << std::endl;
		query join_sink = GetEndToEndQuery(dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->children[1].get(), qid);
		std::cout << "join select " << join_sink.in_select << ", " << join_sink.out_select << " FROM " << join_sink.from << " where " << join_sink.condition << std::endl;

		std::cout <<((PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get())->ToString() << std::endl;
		query Q_distinct = GetEndToEndQuery( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), qid);
		std::cout << "select " << Q_distinct.in_select << ", " << Q_distinct.out_select << " FROM " << Q_distinct.from << " where " << Q_distinct.condition << std::endl;
		query child = GetEndToEndQuery(op->children[0].get(), qid);
		// join child with Q_join right hand side
		std::cout << "select " << child.in_select << ", " << child.out_select << " FROM " << child.from << " where " << child.condition << std::endl;
		string join_table_name =  "LINEAGE_"+ to_string(qid)+"_"+dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->GetName()+"_";
		JoinType join_type = dynamic_cast<PhysicalJoin *>(dynamic_cast<PhysicalDelimJoin *>(op)->join.get())->join_type;
		Q = join_caluse(join_table_name, child, join_sink, op->children[0]->id, dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->children[1].get()->id, join_type);
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		Q = GetEndToEndQuery(op->children[0].get(), qid);
	}
	default: {

	}
	}

	return Q;
}

/*
 * Get the end to end lineage for a query plan
 */
void LineageManager::EndToEndQuery(PhysicalOperator *op) {
	GetEndToEndQuery(op, 0);
}

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 */
void LineageManager::AnnotatePlan(PhysicalOperator *op, bool trace_lineage) {
	std::cout << op->ToString() << std::endl;

	PlanAnnotator(op, 0, trace_lineage);
	std::cout << op->ToString() << std::endl;
/*
	if (trace_lineage) {
		query Q = GetEndToEndQuery(op, 0);
		std::cout << "select " << Q.in_select << ", " << Q.out_select << " FROM " << Q.from << " where " << Q.condition
		          << std::endl;
	}*/
}

// Get the column types for this operator
// Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::ORDER_BY: {
		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
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
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::BIGINT);
		res.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> source_table_columns;
		source_table_columns.emplace_back("in_index", LogicalType::BIGINT);
		source_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source_table_columns));
		break;
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("lhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("rhs_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
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

// Create the table for this operator and fill it with lineage
void LineageManager::CreateLineageTables(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);

	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
							+ op->GetName() + "_" + to_string(i);

		// Create Table
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
		table->Persist(*table, context, insert_chunk);
		while (lps.still_processing) {
			lps = op->lineage_op->Process(table->GetTypes(), lps.count_so_far, insert_chunk);
			table->Persist(*table, context, insert_chunk);
		}
		op->lineage_op->FinishedProcessing();
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		CreateLineageTables( dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		CreateLineageTables( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get());
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(op->children[i].get());
	}
}

/*
 * Create table to store executed queries with their IDs
 * Table name: queries_list
 * Schema: (INT query_id, BLOB query)
 */
void LineageManager::CreateQueryTable() {
	auto info = make_unique <CreateTableInfo>();
	info->schema = DEFAULT_SCHEMA;
	info->table = QUERY_LIST_TABLE_NAME;
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
void LineageManager::LogQuery(const string& input_query) {
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context)
	                             .GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, QUERY_LIST_TABLE_NAME);
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

} // namespace duckdb
#endif
