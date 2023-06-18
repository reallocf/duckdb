#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;

shared_ptr<PipelineLineage> LineageManager::GetPipelineLineageNodeForOp(PhysicalOperator *op, int thd_id) {
	switch (op->type) {
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		return make_shared<PipelineScanLineage>();
	}
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER: {
		return make_shared<PipelineSingleLineage>(op->children[0]->lineage_op[thd_id]->GetPipelineLineage());
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::ORDER_BY: {
		return make_shared<PipelineBreakerLineage>();
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::HASH_JOIN: {
		return make_shared<PipelineJoinLineage>(op->children[0]->lineage_op[thd_id]->GetPipelineLineage());
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		return make_shared<PipelineJoinLineage>(op->children[0]->lineage_op[thd_id]->GetPipelineLineage());
	}
	case PhysicalOperatorType::PROJECTION: {
		// Pass through to last operator
		return op->children[0]->lineage_op[thd_id]->GetPipelineLineage();
	}
	default:
		// Lineage unimplemented! TODO these :)
		return nullptr;
	}
}

// This is used to recursively skip projections
shared_ptr<OperatorLineage> GetThisLineageOp(PhysicalOperator *op, int thd_id) {
	switch (op->type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::DELIM_JOIN: {
		return op->lineage_op[thd_id];
	}
	case PhysicalOperatorType::HASH_JOIN: {
		JoinType join_type = dynamic_cast<PhysicalJoin *>(op)->join_type;
		if (join_type == JoinType::MARK) {
			// Pass through Mark Joins
			return GetThisLineageOp(op->children[0].get(), thd_id);
		} else {
			return op->lineage_op[thd_id];
		}
	}
	case PhysicalOperatorType::PROJECTION: {
		// Skip projection!
		return GetThisLineageOp(op->children[0].get(), thd_id);
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

std::vector<shared_ptr<OperatorLineage>> GetChildrenForOp(PhysicalOperator *op, int thd_id) {
	switch (op->type) {
	case PhysicalOperatorType::CHUNK_SCAN:
	case PhysicalOperatorType::DELIM_SCAN:
	case PhysicalOperatorType::DUMMY_SCAN:
	case PhysicalOperatorType::TABLE_SCAN: {
		return {};
	}
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::SIMPLE_AGGREGATE:
	case PhysicalOperatorType::WINDOW: {
		if (op->children.empty()) {
			// The aggregation in DelimJoin TODO figure this out
			return {};
		} else {
			return {GetThisLineageOp(op->children[0].get(), thd_id)};
		}
	}
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::INDEX_JOIN: {
		return {
			GetThisLineageOp(op->children[0].get(), thd_id),
			GetThisLineageOp(op->children[1].get(), thd_id)
		};

	}
	case PhysicalOperatorType::HASH_JOIN: {
		// Hash joins are all flipped around
		return {
		    GetThisLineageOp(op->children[1].get(), thd_id),
		    GetThisLineageOp(op->children[0].get(), thd_id)
		};
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// TODO think through this more deeply - will probably require re-arranging some child pointers
		PhysicalDelimJoin *delim_join = dynamic_cast<PhysicalDelimJoin *>(op);
		return {
		    GetThisLineageOp(op->children[0].get(), thd_id),
			GetThisLineageOp(delim_join->join.get(), thd_id),
			GetThisLineageOp((PhysicalOperator *)delim_join->distinct.get(), thd_id),
			// TODO DelimScans...
		};
	}
	case PhysicalOperatorType::PROJECTION: {
		// Set projection if it's the root (others will be skipped!)
		return {GetThisLineageOp(op->children[0].get(), thd_id)};
	}
	default:
		// Lineage unimplemented! TODO these :)
		return {};
	}
}

void LineageManager::CreateOperatorLineage(PhysicalOperator *op, int thd_id, bool trace_lineage, bool should_index) {
	should_index = should_index
	               && op->type != PhysicalOperatorType::ORDER_BY; // Order by is one chunk, so no need to index
	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		auto distinct = (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		CreateOperatorLineage( distinct, thd_id, trace_lineage, true);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]->lineage_op  = distinct->lineage_op;
		}
		CreateOperatorLineage( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), thd_id, trace_lineage, true);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		bool child_should_index =
		    op->type == PhysicalOperatorType::HASH_GROUP_BY
		    || op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY
		    || (op->type == PhysicalOperatorType::HASH_JOIN && i == 1) // Only build side child needs an index
		    || (op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::CROSS_PRODUCT && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::NESTED_LOOP_JOIN && i == 1) // Right side needs index
		    || (op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN && i == 1) // Right side needs index
		    || op->type == PhysicalOperatorType::ORDER_BY
		    || (op->type == PhysicalOperatorType::DELIM_JOIN && i == 0) // Child zero needs an index
		    || (op->type == PhysicalOperatorType::PROJECTION && should_index); // Pass through should_index on projection
		CreateOperatorLineage(op->children[i].get(), thd_id, trace_lineage, child_should_index);
	}
	op->lineage_op[thd_id] = make_shared<OperatorLineage>(
	    op->id,
	    trace_lineage,
	    GetPipelineLineageNodeForOp(op, thd_id),
	    GetChildrenForOp(op, thd_id),
	    op->type,
	    should_index
	);
	if (
	    op->type == PhysicalOperatorType::HASH_JOIN ||
	    op->type == PhysicalOperatorType::PIECEWISE_MERGE_JOIN ||
	    op->type == PhysicalOperatorType::NESTED_LOOP_JOIN ||
	    op->type == PhysicalOperatorType::BLOCKWISE_NL_JOIN
	) {
		// Cache join type so we can avoid anti joins
		op->lineage_op[thd_id]->join_type = dynamic_cast<PhysicalJoin *>(op)->join_type;
	}
	if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_str = dynamic_cast<PhysicalTableScan *>(op)->ParamsToString();
		// TODO there's probably a better way to do this...
		op->lineage_op[thd_id]->table_name = table_str.substr(0, table_str.find('\n'));
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
	return counter + 1;
}

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 *
 * CreateOperatorLineage: allocate lineage_op for main thread (id=-1)
 */
void LineageManager::InitOperatorPlan(PhysicalOperator *op, bool trace_lineage) {
	PlanAnnotator(op, 0, trace_lineage);
	CreateOperatorLineage(op, -1, trace_lineage, true); // Always index root
}

// Get the column types for this operator
// Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> LineageManager::GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::ORDER_BY: {
		// schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> table_columns;
		table_columns.emplace_back("rowid", LogicalType::INTEGER);
		table_columns.emplace_back("in_index", LogicalType::INTEGER);
		table_columns.emplace_back("out_index", LogicalType::INTEGER);
		table_columns.emplace_back("Thread_id", LogicalType::INTEGER);
		res.emplace_back(move(table_columns));
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// sink schema: [INTEGER in_index, INTEGER out_index]
		vector<ColumnDefinition> sink_table_columns;
		sink_table_columns.emplace_back("in_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("out_index", LogicalType::INTEGER);
		sink_table_columns.emplace_back("thread_id", LogicalType::INTEGER);
		res.emplace_back(move(sink_table_columns));
		// source schema: [INTEGER in_index, INTEGER out_index]
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
		string index_col_name = table_column_types[i][0].name;

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
		bound_create_info->isLineageTable = true;
		auto &catalog = Catalog::GetCatalog(context);
		TableCatalogEntry *table =
			dynamic_cast<TableCatalogEntry *>(catalog.CreateTable(context, bound_create_info.get()));


//		//Create Index
//		auto index_info = make_unique<CreateIndexInfo>();
//		auto tableref = make_unique<BaseTableRef>();
//		tableref->table_name = table_name;
//		tableref->schema_name = table->schema->name;
//		index_info->index_name = table_name  + "_INDEX";
//		index_info->index_type = IndexType::LINEAGE_INDEX;
//		index_info->table = move(tableref);
//
//		//auto schema_entry = table->schema->CreateIndex(context, index_info.get(), table);
//		auto index_entry = (IndexCatalogEntry *)table->schema->CreateIndex(context, index_info.get(), table);
//		if (!index_entry) {
//			// index already exists, but error ignored because of IF NOT EXISTS
//			continue;
//		}
//		DataTable* dataTable = table->storage.get();
//		vector<unique_ptr<Expression>> exps;
//		unique_ptr<ParsedExpression> exp = make_unique<ColumnRefExpression>(index_col_name,  table_name);
//		exp->type = ExpressionType::COLUMN_REF;
//		exp->expression_class = ExpressionClass::COLUMN_REF;
//		exp->alias = index_col_name;
//		//exp->table_name = table_name;
//		//exp->column_name = table_column_types[0][0].name;
//		IndexBinder indexBinder(*Binder::CreateBinder(this->context), this->context);
//		exps.emplace_back(indexBinder.Bind(exp));
//
//		vector<column_t> col_ids;
//		col_ids.push_back(0);
//		unique_ptr<Index> index = make_unique<LineageIndex>(col_ids, exps);
//
//		dataTable->AddIndex(move(index), exps);
//
//






		table->opLineage = op->lineage_op.at(-1);

		/*// Persist Data
		DataChunk insert_chunk;
		vector<LogicalType> types = table->GetTypes();
		insert_chunk.Initialize(types);
		// Local statistics per operator
		idx_t op_count = 0;
		idx_t op_size = 0;
		for (auto const& lineage_op : op->lineage_op) {
			LineageProcessStruct lps = lineage_op.second->Process(table->GetTypes(), 0, insert_chunk, 0, lineage_op.first);
			if (lps.count_so_far) {
				op_count++;
				table->Persist(*table, context, insert_chunk);
			}

			while (lps.still_processing) {
				lps = lineage_op.second->Process(table->GetTypes(), lps.count_so_far, insert_chunk, lps.size_so_far, lineage_op.first);
				if (lps.count_so_far) {
					op_count++;
					table->Persist(*table, context, insert_chunk);
				}
			}
			lineage_op.second->FinishedProcessing();
			op_size += lps.size_so_far;
			total_size += op_size;
		}*/
		//std::cout << table_name << " count: " << op_count << " size: " << op_size << std::endl;
	}

	//std::cout << "total size: " << total_size  << std::endl;
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

void LineageManager::CreateRelationalLineageTable(const shared_ptr<PhysicalOperator>& op) {
	// Create full relational lineage table with name like LINEAGE_1
	string table_name = "LINEAGE_" + to_string(query_id);
	vector<string> lineage_table_names = GetLineageTableNames(op.get());

	auto info = make_unique<CreateTableInfo>();
	info->schema = DEFAULT_SCHEMA;
	info->table = table_name;
	info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	info->temporary = false;
	for (const string& name : lineage_table_names) {
		info->columns.emplace_back(name, lineage_col_type);
	}
	info->columns.emplace_back("out_col", lineage_col_type);
	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(info));
	bound_create_info->lineage_query_as_table = true;
	bound_create_info->isLineageTable = true;
	auto &catalog = Catalog::GetCatalog(context);
	TableCatalogEntry *table =
	    dynamic_cast<TableCatalogEntry *>(catalog.CreateTable(context, bound_create_info.get()));

	table->lineage_id = query_id;
	// TODO: handle multithreading
	table->lineage_output_count = GetLineageOpSize(op->lineage_op.at(-1).get());
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
	info->columns.emplace_back("query", LogicalType::VARCHAR);
	info->columns.emplace_back("lineage_size", LogicalType::UBIGINT);

	auto binder = Binder::CreateBinder(context);
	auto bound_create_info = binder->BindCreateTableInfo(move(info));
	auto &catalog = Catalog::GetCatalog(context);
	catalog.CreateTable(context, bound_create_info.get());
}

/*
 * Persist executed query in queries_list table
 */
idx_t LineageManager::LogQuery(const string& input_query, idx_t lineage_size) {
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context)
	                             .GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, QUERY_LIST_TABLE_NAME);
  DataChunk insert_chunk;
  insert_chunk.Initialize(table->GetTypes());
  insert_chunk.SetCardinality(count);

  // query id
  idx_t this_query_id = query_id;
  Vector query_ids(Value::INTEGER(this_query_id));
  Vector lineage_size_vec(Value::UBIGINT(lineage_size));

  // query value
  Vector payload(input_query);

  // populate chunk
  insert_chunk.data[0].Reference(query_ids);
  insert_chunk.data[1].Reference(payload);
  insert_chunk.data[2].Reference(lineage_size_vec);

  table->Persist(*table, context, insert_chunk);

  return this_query_id;
}

void LineageManager::IncrementQueryId() {
	query_id++;
}

string FindNextLineageTableName(const unordered_set<string>& so_far, string name) {
	string orig_name = name;
	idx_t j = 0;
	while (so_far.count(name) > 0) {
		if (orig_name == name) {
			// Add suffix for the first time
			name = name + "_" + to_string(j++);
		} else {
			// Remove latest number and add the next one
			name = name.substr(0, name.size() - 1) + to_string(j++);
		}
	}
	return name;
}

vector<string> GetLineageTableNames(PhysicalOperator *op) {
	vector<string> res;
	unordered_set<string> res_set;

	for (idx_t i = 0; i < op->children.size(); i++) {
		vector<string> found = GetLineageTableNames(op->children[i].get());
		// Resolve naming conflicts (self-joins)
		// We do not always have an available alias, for example when there's a semi-join like
		// select * from foo where id in (select id from foo) so instead we name these:
		// foo, foo_1, foo_2, ...
		// Note that we don't do foo_0 since many queries do not have self-joins, so we avoid the ugly suffix when possible
		for (string name : found) {
			name = FindNextLineageTableName(res_set, name);
			res.push_back(name);
			res_set.insert(name);
		}
	}

	// TODO: multithreading
	string table_name = op->lineage_op[-1]->table_name;
	if (!table_name.empty()) {
		table_name = FindNextLineageTableName(res_set, table_name);
		res.push_back(table_name);
		res_set.insert(table_name);
	}

	return res;
}

idx_t GetLineageOpSize(OperatorLineage *op) {
	switch (op->type) {
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::INDEX_JOIN:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY:
	case PhysicalOperatorType::TABLE_SCAN: {
		return op->GetThisOffset(LINEAGE_UNARY);
	}
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN: {
		return op->GetThisOffset(LINEAGE_PROBE);
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		return op->GetThisOffset(LINEAGE_SOURCE);
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		return 1;
	}
	case PhysicalOperatorType::PROJECTION: {
		return GetLineageOpSize(op->children[0].get());
	}
	case PhysicalOperatorType::PRAGMA: {
		return 0; // No op - we see this for PRAGMA TRACE_LINEAGE = 'ON'
	}
	default: {
		throw std::logic_error("Unsupported PhysicalOperatorType for relational lineage querying");
	}
	}
}

} // namespace duckdb
#endif
