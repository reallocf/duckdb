#ifdef LINEAGE
#include "duckdb/execution/lineage_context.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

namespace duckdb {
class PhysicalOperator;
class LineageContext;


void ManageLineage::Reset() {
  /* starting a new query */
  pipelines_lineage.clear();
  op_id = 0;
  query_id++;
}

/*
 * Persist lineage in-memory for normal execution path
 * */
void ManageLineage::AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext> lineage) {
  if (lineage->isEmpty() == false) {
    pipelines_lineage[0][opKey].push_back(move(lineage));
  }
}

/*
 * Persist lineage in-memory for Sink based operators
 * */
void ManageLineage::AddLocalSinkLineage(PhysicalOperator* sink,  vector<shared_ptr<LineageContext>> lineage_vec) {
  if (lineage_vec.size() > 0) {
    pipelines_lineage[1][sink].reserve(pipelines_lineage[1][sink].size() + lineage_vec.size());
    pipelines_lineage[1][sink].insert(pipelines_lineage[1][sink].end(), lineage_vec.begin(), lineage_vec.end());
  }
}

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 */
void ManageLineage::AnnotatePlan(PhysicalOperator *op) {
  if (!op) return;
  op->id = op_id++;
#ifdef LINEAGE_DEBUG
  std::cout << op->GetName() << " " << op->id << std::endl;
#endif
  for (idx_t i = 0; i < op->children.size(); ++i)
    AnnotatePlan(op->children[i].get());
}

/*
 * Create table to store executed queries with their IDs
 * Table name: queries_list
 * Schema: (INT query_id, BLOB query)
 */
void ManageLineage::CreateQueryTable() {
  auto info = make_unique <CreateTableInfo>();
  info->schema = DEFAULT_SCHEMA;
  info->table = "queries_list";
  info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
  info->temporary = false;

  info->columns.push_back(ColumnDefinition("query_id", LogicalType::INTEGER));
  info->columns.push_back(ColumnDefinition("query", LogicalType::BLOB));

  auto binder = Binder::CreateBinder(context);
  auto bound_create_info = binder->BindCreateTableInfo(move(info));
  auto &catalog = Catalog::GetCatalog(context);
  catalog.CreateTable(context, bound_create_info.get());
  queries_list_table_set = true;
}

/*
 * Persist executed query in queries_list table
 */
void ManageLineage::logQuery(string input_query) {
  if (!queries_list_table_set) {
    CreateQueryTable();
  }

  string tablename = "queries_list";
  idx_t count = 1;
  TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
  DataChunk insert_chunk;
  insert_chunk.Initialize(table->GetTypes());
  insert_chunk.SetCardinality(count);

  // query id
  Vector query_ids(Value::INTEGER(query_id));

  // query value
  Vector payload(Value::BLOB(input_query));

  // populate chunk
  insert_chunk.data[0].Reference(query_ids);
  insert_chunk.data[1].Reference(payload);

  table->Persist(*table, context, insert_chunk);
}

void ManageLineage::CreateLineageTables(PhysicalOperator *op) {
	string base = op->GetName() + "_" + to_string(query_id) + "_" + to_string( op->id );
  switch (op->type) {
  case PhysicalOperatorType::LIMIT: {
    // schema: [INT limit, INT offset, INT out_chunk_id]
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("offset", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("limit", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());
    CreateLineageTables(op->children[0].get());
    break;
  }
  case PhysicalOperatorType::FILTER: {
    // CREATE TABLE base:
    // schema: [INT in_index, INT out_index, INT out_chunk_id]
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("in_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());
    CreateLineageTables(op->children[0].get());
    break;
  }
	case PhysicalOperatorType::TABLE_SCAN: {
    // CREATE TABLE base_range (range_start INTEGER, range_end INTEGER, chunk_id INTEGER)
    auto info = make_unique<CreateTableInfo>();
    // CREATE TABLE base:
    // schema: [BOOLEAN filter_exists, INT in_index, INT out_index, INT in_chunk_id, INT out_chunk_id]
    info->schema = DEFAULT_SCHEMA;
    info->table = base;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    // if filter_exists is set, then consider in_index and out_index
    // else, just the in_chunk_id would matter
    info->columns.push_back(ColumnDefinition("filter_exists", LogicalType::BOOLEAN));
    info->columns.push_back(ColumnDefinition("in_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("in_chunk_id", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());
    break;
  }
  case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
  case PhysicalOperatorType::HASH_GROUP_BY: {
    // CREATE TABLE base_out (group_id INTEGER, out_index INTEGER, out_chunk_id INTEGER)
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base + "_PROBE";
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("group_id", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());

    // CREATE TABLE base_sink (group_id INTEGER, in_index INTEGER, out_chunk_id INTEGER)
    info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base + "_SINK";
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("group_id", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("in_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    bound_create_info = binder->BindCreateTableInfo(move(info));
    catalog.CreateTable(context, bound_create_info.get());

    CreateLineageTables(op->children[0].get());
		break;
  }
  case PhysicalOperatorType::INDEX_JOIN: {
    // CREATE TABLE INDEX_JOIN (lhs_value INT, rhs_value BIGINT, in_index INT, out_chunk_id INT)
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("lhs_value", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("rhs_value", LogicalType::BIGINT));
    info->columns.push_back(ColumnDefinition("in_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());

    CreateLineageTables(op->children[0].get());
    CreateLineageTables(op->children[1].get());
    break;
  }
  case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
  case PhysicalOperatorType::NESTED_LOOP_JOIN: {
    // CREATE TABLE rhs_sink:
    // schema: [INT in_chunk_id, INT out_chunk_id]
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base +  "_SINK";
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("in_chunk_id", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());

    // CREATE TABLE base:
    // schema: [INT lhs_value, INT rhs_value, INT rhs_chunk_id, INT out_index, INT out_chunk_id]
    info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("lhs_value", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("rhs_value", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("rhs_chunk_id", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    bound_create_info = binder->BindCreateTableInfo(move(info));
    catalog.CreateTable(context, bound_create_info.get());

    CreateLineageTables(op->children[0].get());
    CreateLineageTables(op->children[1].get());
    break;
  }
  case PhysicalOperatorType::HASH_JOIN: {
    // CREATE TABLE base_PROBE:
    // schema: [INT out_index, INT lhs_value, BIGINT rhs_address, INT out_chunk_id]
    auto info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base +  "_PROBE";
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("lhs_value", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("rhs_address", LogicalType::BIGINT));
    info->columns.push_back(ColumnDefinition("out_index", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());

    // CREATE TABLE base_SINK:
    // schema: [INT rhs_value, BIGINT rhs_address, INT out_chunk_id]
    info = make_unique<CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = base +  "_SINK";
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;
    info->columns.push_back(ColumnDefinition("rhs_address", LogicalType::BIGINT));
    info->columns.push_back(ColumnDefinition("rhs_value", LogicalType::INTEGER));
    info->columns.push_back(ColumnDefinition("out_chunk_id", LogicalType::INTEGER));
    bound_create_info = binder->BindCreateTableInfo(move(info));
    catalog.CreateTable(context, bound_create_info.get());

    CreateLineageTables(op->children[0].get());
    CreateLineageTables(op->children[1].get());
    break;
  } default:
    for (idx_t i = 0; i < op->children.size(); ++i)
      CreateLineageTables(op->children[i].get());
  }
}

void ManageLineage::Persist(PhysicalOperator *op, shared_ptr<LineageContext> lineage, bool is_sink = false) {
  string tablename = op->GetName() + "_" + to_string(query_id) + "_" + to_string( op->id );
  switch (op->type) {
  case PhysicalOperatorType::LIMIT: {
    LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op->id, 0).get());
    if (!lop) return;
    // schema: [INT limit, INT offset, INT out_chunk_id]
    TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
    DataChunk insert_chunk;
    insert_chunk.Initialize(table->GetTypes());
    insert_chunk.SetCardinality(1);

    Vector offset(Value::Value::INTEGER(dynamic_cast<LineageRange&>(*lop->data).start));
    Vector limit(Value::Value::INTEGER(dynamic_cast<LineageRange&>(*lop->data).end));
    Vector out_chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

    insert_chunk.data[0].Reference(offset);
    insert_chunk.data[1].Reference(limit);
    insert_chunk.data[2].Reference(out_chunk_ids);

    table->Persist(*table, context, insert_chunk);
    Persist(op->children[0].get(), move(lineage), is_sink);
    break;
  }
  case PhysicalOperatorType::FILTER: {
    LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op->id, 0).get());
    if (!lop) return;
    // schema: [INT in_index, INT out_index, INT out_chunk_id]
    TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
    DataChunk insert_chunk;
    insert_chunk.Initialize(table->GetTypes());
    idx_t count = dynamic_cast<LineageSelVec&>(*lop->data).count;
    insert_chunk.SetCardinality(count);

    Vector payload(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*lop->data).vec[0]);
    Vector out_chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

    insert_chunk.data[0].Reference(payload);
    insert_chunk.data[1].Sequence(0, 1);
    insert_chunk.data[2].Reference(out_chunk_ids);

    table->Persist(*table, context, insert_chunk);
    Persist(op->children[0].get(), move(lineage), is_sink);
    break;
  }
  case PhysicalOperatorType::TABLE_SCAN: {
    // schema: [BOOLEAN filter_exists, INT in_index, INT out_index, INT in_chunk_id, INT out_chunk_id]
    LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op->id, 0).get());
    if (!lop) return;

    std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
    // need to adjust the offset based on start
    bool filter_exists = false;
    idx_t count = 1;
    if (collection->collection.find("vector_index") != collection->collection.end()) {
      auto vector_index = dynamic_cast<LineageConstant&>(*collection->collection["vector_index"]).value;
      if (collection->collection.find("filter") != collection->collection.end()) {
        filter_exists = true;
        count = dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).count;
      }

      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      insert_chunk.SetCardinality(count);

      if (filter_exists) {
        Vector filter_payload(table->GetTypes()[1], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).vec[0]);
        insert_chunk.data[1].Reference(filter_payload);
      } else {
        Vector filter_payload(Value::Value::INTEGER(0));
        insert_chunk.data[1].Reference(filter_payload);
      }

      Vector filter_exists_set(Value::Value::BOOLEAN(filter_exists));
      Vector out_chunk_ids(Value::Value::INTEGER(lineage->chunk_id));
      Vector in_chunk_ids(Value::Value::INTEGER(vector_index));

      insert_chunk.data[0].Reference(filter_exists_set);
      insert_chunk.data[2].Sequence(0, 1);
      insert_chunk.data[3].Reference(in_chunk_ids); // Adjust using STANDARD_VECTOR_SIZE
      insert_chunk.data[4].Reference(out_chunk_ids);

      table->Persist(*table, context, insert_chunk);
    }

    break;
  }
  case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
  case PhysicalOperatorType::HASH_GROUP_BY: {
    if (is_sink) {
      std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 1));
      if (!sink_lop) return;
      // schema: [INT in_index, INT group_id, INT out_chunk_id]
      //         map input rowid (index) to a unique group in the HT (group).
      //         chunk_id is used to associate a lineage for this operator to
      //         its children.
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename + "_SINK");
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      idx_t count = dynamic_cast<LineageSelVec&>(*sink_lop->data).count;
      insert_chunk.SetCardinality(count);

      Vector sink_payload(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*sink_lop->data).vec[0]);
      Vector chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

      insert_chunk.data[0].Reference(sink_payload);
      insert_chunk.data[1].Sequence(0, 1);
      insert_chunk.data[2].Reference(chunk_ids);

      table->Persist(*table, context, insert_chunk);
    } else {
      std::shared_ptr<LineageOpUnary> lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) return;
      // schema: [INT out-index, INT group_id, INT out_chunk_id]
      //         map output rowid (index) to the unique group in the HT (group).
      //         chunk_id is used to associate a lineage for this operator to
      //         its children.
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename + "_PROBE");
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      idx_t count = dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).count;
      insert_chunk.SetCardinality(count);

      Vector payload(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).vec[0]);
      Vector chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

      insert_chunk.data[0].Reference(payload);
      insert_chunk.data[1].Sequence(0, 1);
      insert_chunk.data[2].Reference(chunk_ids);

      table->Persist(*table, context, insert_chunk);
      Persist(op->children[0].get(), lineage, is_sink);
    }
    break;
  }
  case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
  case PhysicalOperatorType::NESTED_LOOP_JOIN: {
    // schema: [INT in_chunk_id, INT out_chunk_id]
    if (is_sink) {
      std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 1));
      if (!sink_lop) {
          return;
      }
      // schema: [INT rhs_value, BIGINT rhs_address, INT out_chunk_id]
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename + "_SINK");
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      insert_chunk.SetCardinality(1);

      // build side - group RHS to unique HT address
      Vector in_chunk_ids(Value::Value::INTEGER(dynamic_cast<LineageConstant&>(*sink_lop->data).value));
      Vector out_chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

      insert_chunk.data[0].Reference(in_chunk_ids);
      insert_chunk.data[1].Reference(out_chunk_ids);

      table->Persist(*table, context, insert_chunk);
    } else {
      // schema: [INT lhs_value, INT rhs_value, INT rhs_chunk_id, INT out_index, INT out_chunk_id]
      std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) {
          return;
      }
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      idx_t count = dynamic_cast<LineageSelVec&>(*lop->data_lhs).count;
      insert_chunk.SetCardinality(count);

      Vector lhs_value(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*lop->data_lhs).vec[0]);
      Vector rhs_value(table->GetTypes()[1], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*lop->data_rhs).vec[0]);
      Vector rhs_chunk_id(Value::Value::INTEGER(dynamic_cast<LineageSelVec&>(*lop->data_rhs).offset));
      Vector chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

      insert_chunk.data[0].Reference(lhs_value);
      insert_chunk.data[1].Reference(rhs_value);
      insert_chunk.data[2].Reference(rhs_chunk_id);
      insert_chunk.data[3].Sequence(0, 1);
      insert_chunk.data[4].Reference(chunk_ids);

      table->Persist(*table, context, insert_chunk);
      Persist(op->children[0].get(), lineage, is_sink);
    }
    break;
  }
  case PhysicalOperatorType::HASH_JOIN: {
    if (is_sink) {
      std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 1));
      if (!sink_lop) {
          return;
      }
      // schema: [INT rhs_value, BIGINT rhs_address, INT out_chunk_id]
      //         map input rowid (index) to a unique address in the HT (value)
      //         chunk_id is used to associate a lineage for this operator to
      //         its children.
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename + "_SINK");
      DataChunk insert_chunk;
      insert_chunk.Initialize(table->GetTypes());
      idx_t count = dynamic_cast<LineageDataArray<uintptr_t>&>(*sink_lop->data).count;
      insert_chunk.SetCardinality(count);

      // build side - group RHS to unique HT address
      Vector sink_payload(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageDataArray<uintptr_t>&>(*sink_lop->data).vec[0]);
      Vector chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

      insert_chunk.data[0].Reference(sink_payload);
      insert_chunk.data[1].Sequence(0, 1);
      insert_chunk.data[2].Reference(chunk_ids);

      table->Persist(*table, context, insert_chunk);
    } else {
      std::shared_ptr<LineageOpCollection> probe_lop = std::dynamic_pointer_cast<LineageOpCollection>(lineage->GetLineageOp(op->id, 0));
      if (!probe_lop) {
          return;
      }
      // schema: [INT in_index, INT lhs_value, BIGINT rhs_address, INT out_chunk_id]
      //         maps output row to row from probe side (LHS) and what the
      //         unique group address it maps to from the RHS
      TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename + "_PROBE");
      for (idx_t i = 0; i < probe_lop->op.size(); ++i) {
        auto op = dynamic_cast<LineageOpBinary&>(*probe_lop->op[i]);
        idx_t count = dynamic_cast<LineageSelVec&>(*op.data_lhs).count;
        DataChunk insert_chunk;
        insert_chunk.Initialize(table->GetTypes());
        insert_chunk.SetCardinality(count);

        // probe - LHS
        Vector lhs_payload(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*op.data_lhs).vec[0]);
        // build side - RHS
        Vector rhs_payload(table->GetTypes()[1], (data_ptr_t)&dynamic_cast<LineageDataArray<uintptr_t>&>(*op.data_rhs).vec[0]);
        Vector chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

        insert_chunk.data[0].Reference(lhs_payload);
        insert_chunk.data[1].Reference(rhs_payload);
        insert_chunk.data[2].Sequence(0, 1);
        insert_chunk.data[3].Reference(chunk_ids);

        table->Persist(*table, context, insert_chunk);
      }

      Persist(op->children[0].get(), lineage, is_sink);
      Persist(op->children[1].get(), lineage, is_sink);
    }
    break;
  }
  case PhysicalOperatorType::INDEX_JOIN: {
    // CREATE TABLE INDEX_JOIN (lhs_value INT, rhs_value BIGINT, in_index INT, out_chunk_id INT)
    LineageOpBinary *lop = dynamic_cast<LineageOpBinary *>(lineage->GetLineageOp(op->id, 0).get());
    if (!lop) return;
    TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
    DataChunk insert_chunk;
    insert_chunk.Initialize(table->GetTypes());
    idx_t count = dynamic_cast<LineageSelVec&>(*lop->data_lhs).count;
    insert_chunk.SetCardinality(count);

    Vector lhs_values(table->GetTypes()[0], (data_ptr_t)&dynamic_cast<LineageSelVec&>(*lop->data_lhs).vec[0]);
    Vector rhs_values(table->GetTypes()[1], (data_ptr_t)&dynamic_cast<LineageDataVector<row_t>&>(*lop->data_rhs).vec[0]);
    Vector out_chunk_ids(Value::Value::INTEGER(lineage->chunk_id));

    insert_chunk.data[0].Reference(lhs_values);
    insert_chunk.data[1].Reference(rhs_values);
    insert_chunk.data[2].Sequence(0, 1);
    insert_chunk.data[3].Reference(out_chunk_ids);

    table->Persist(*table, context, insert_chunk);
    Persist(op->children[0].get(), move(lineage), is_sink);
    break;
  }
  default:
    for (idx_t i = 0; i < op->children.size(); ++i)
      Persist(op->children[i].get(), lineage, is_sink);
  }
}

void ManageLineage::BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx) {
  std::cout << "Backward Lineage: TraverseTree op " << op << " " << op->GetName() << std::endl;
  switch (op->type) {
    case PhysicalOperatorType::LIMIT: {
      LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op->id, 0).get());
      if (!lop) {
        std::cout << "something is wrong, lop not found for  limit" << std::endl;
        return;
      }

      auto offset = dynamic_cast<LineageRange&>(*lop->data).start;
      auto limit = dynamic_cast<LineageRange&>(*lop->data).end;
      std::cout << "Limit: offset " << offset << " limit: " << limit << std::endl;
      BackwardLineage(op->children[0].get(), lineage, oidx);
      break;
    }
    case PhysicalOperatorType::TABLE_SCAN: {
      LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op->id, 0).get());
      if (!lop) {
        std::cout << "something is wrong, lop not found for  table scan" << std::endl;
        return;
      }
      std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
      // need to adjust the offset based on start
      if (collection->collection.find("vector_index") != collection->collection.end()) {
        auto vector_index = dynamic_cast<LineageConstant&>(*collection->collection["vector_index"]).value;
        std::cout << "Table scan chunk Id " << vector_index <<  std::endl;
      }

      if (collection->collection.find("filter") != collection->collection.end()) {
        // get selection vector
        std::cout << "filter on scan" <<  std::endl;
        auto fidx = dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).getAtIndex(oidx);
        std::cout << oidx << " maps to " << fidx  << std::endl;
      }
      break;
    }
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
      std::shared_ptr<LineageOpUnary> lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) {
          std::cout << "something is wrong, lop not found for aggregate" << std::endl;
          return;
      }

      // schema: [oidx idx_t, group idx_t]
      //         maps a row in the output to a group
      idx_t group =  dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).getAtIndex(oidx);
      std::cout << oidx << " belong to " << group << std::endl;

      // Lookup the data on the build side
      if (pipelines_lineage[1].find(op) != pipelines_lineage[1].end()) {
          vector<shared_ptr<LineageContext>> sink_lineage = pipelines_lineage[1][op];
          vector<idx_t> matches;
          for (idx_t i = 0; i < sink_lineage.size(); ++i) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(sink_lineage[i]->GetLineageOp(op->id, 1));

            if (!sink_lop) {
              std::cout << "something is wrong, aggregate sink lop not found" << std::endl;
              continue;
            }

            // schema: [ridx idx_t, group idx_t]
            //         maps input row to a specific group
            // getAllMatches: get all ridx that belong to group, O(n)
            dynamic_cast<LineageSelVec&>(*sink_lop->data).getAllMatches(group, matches);
            std::cout << " getAllMatches for " << group << " has " << matches.size() << std::endl;
          }
        }
        BackwardLineage(op->children[0].get(), lineage, oidx);
        break;
    }
    case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
    case PhysicalOperatorType::NESTED_LOOP_JOIN: {
      std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) {
          std::cout << "something is wrong, " << op->GetName() << " not found" << std::endl;
          return;
      }
      idx_t lhs_value =  dynamic_cast<LineageSelVec&>(*lop->data_lhs).getAtIndex(oidx);
      idx_t rhs_value =  dynamic_cast<LineageSelVec&>(*lop->data_rhs).getAtIndex(oidx);
      idx_t rhs_offset = dynamic_cast<LineageSelVec&>(*lop->data_rhs).offset;
      std::cout << "lhs_value " << lhs_value << " rhs_value " << rhs_value << " rhs_offset " << rhs_offset << std::endl;
      BackwardLineage(op->children[0].get(), lineage, oidx);
      break;
    } case PhysicalOperatorType::HASH_JOIN: {
      // Probe lineage can have multiple lineage data if the output is cached
      std::shared_ptr<LineageOpCollection> lop_col = std::dynamic_pointer_cast<LineageOpCollection>(lineage->GetLineageOp(op->id, 0));
      if (!lop_col) {
          std::cout << "something is wrong, hash join build lop not found" << std::endl;
          return;
      }

      for (idx_t i = 0; i < lop_col->op.size(); ++i) {
        auto lop = dynamic_cast<LineageOpBinary&>(*lop_col->op[i]);
        // schema: [oidx idx_t, lhs_idx idx_t]
        //         maps output row to row from probe side (LHS)
        auto lhs_idx =  dynamic_cast<LineageSelVec&>(*lop.data_lhs).getAtIndex(oidx);
        std::cout << "-> Hash Join LHS " <<  lhs_idx << std::endl;

        // schema: [oidx idx_t, rhs_ptr uintptr_t]
        //         maps output row to row from the build side in the hash table payload
        uintptr_t rhs_ptr = dynamic_cast<LineageDataArray<uintptr_t>&>(*lop.data_rhs).getAtIndex(oidx);
        std::cout << "-> Hash Join RHS ptr in HashJoin table " << rhs_ptr<< std::endl;

        // We need to get the actual row id from the build side
        if (pipelines_lineage[1].find(op) != pipelines_lineage[1].end()) {
          vector<shared_ptr<LineageContext>> sink_lineage = pipelines_lineage[1][op];
          for (idx_t i = 0; i < sink_lineage.size(); ++i) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(sink_lineage[i]->GetLineageOp(op->id, 1));

            if (!sink_lop) {
                std::cout << "something is wrong, hash join sink lop not found" << std::endl;
                continue;
            }

            int rhs_idx = dynamic_cast<LineageDataArray<uintptr_t>&>(*sink_lop->data).findIndexOf((idx_t)rhs_ptr);
            std::cout << "rhs_idx " << i << " " << rhs_idx << " " << rhs_ptr << std::endl;
          }
        }
      }
      BackwardLineage(op->children[0].get(), lineage, oidx);
      BackwardLineage(op->children[1].get(), lineage, oidx);
      break;
    }
    case PhysicalOperatorType::INDEX_JOIN: {
      std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) {
          std::cout << "something is wrong, lop not found" << std::endl;
          return;
      }

      auto lhs_idx = dynamic_cast<LineageSelVec&>(*lop->data_lhs).getAtIndex(oidx);
      std::cout << "-> Index Join LHS " <<  lhs_idx << std::endl;
      auto rhs_idx =  dynamic_cast<LineageDataVector<row_t>&>(*lop->data_rhs).getAtIndex(oidx);
      std::cout << "-> Index Join RHS " <<  rhs_idx << std::endl;

      BackwardLineage(op->children[0].get(), lineage, oidx);
      BackwardLineage(op->children[1].get(), lineage, oidx);
      break;
    }
    default: {
      for (idx_t i = 0; i < op->children.size(); ++i)
        BackwardLineage(op->children[i].get(), lineage, oidx);
    }
  }
}

} // namespace duckdb
#endif
