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
	query_id++;
    op_metadata.clear();
    pipelines_lineage.clear();

}

void ManageLineage::AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext> lineage) {
    if (lineage->isEmpty() == false) {
        // need to associate output chunkid with this
        pipelines_lineage[0][opKey].push_back(move(lineage));
    }
}

void ManageLineage::AddLocalSinkLineage(PhysicalOperator* sink,  shared_ptr<LineageContext> lineage) {
    if (lineage->isEmpty() == false) {
        //lock_guard<mutex> elock(executor_lock);
        pipelines_lineage[1][sink].push_back(move(lineage));
    }
}

void ManageLineage::AnnotatePlan(PhysicalOperator *op) {
    if (!op) return;
    if ( op_metadata.find(op->GetName()) == op_metadata.end() )
        op_metadata[op->GetName()] =  0;
    else
        op_metadata[op->GetName()]++;
    op->id = op_metadata[op->GetName()];

    for (int i = 0; i < op->children.size(); ++i)
        AnnotatePlan(op->children[i].get());
}

void ManageLineage::setQuery(string input_query) {
    if (!create_table_exists) {
        // create schema if it doesnt exists
        CreateQueryTable(context);
        create_table_exists = true;
    }

    string tablename = query_table;
    idx_t count = 1;
    TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
    DataChunk insert_chunk;
    insert_chunk.Initialize(table->GetTypes());
    insert_chunk.SetCardinality(count);

    // query id
    Vector query_ids;
    query_ids.SetType(table->GetTypes()[0]);
    query_ids.Reference( Value::INTEGER(query_id));


    // query value
    Vector payload;
    payload.SetType(table->GetTypes()[1]);
    payload.Reference( Value(input_query));


    // populate chunk
    insert_chunk.data[0].Reference(query_ids);
    insert_chunk.data[1].Reference(payload);

    table->Persist(*table, context, insert_chunk);
}

void ManageLineage::CreateQueryTable(ClientContext &context) {
    auto info = make_unique <CreateTableInfo>();
    info->schema = DEFAULT_SCHEMA;
    info->table = query_table;
    info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
    info->temporary = false;

    info->columns.push_back(move(ColumnDefinition("query_id", LogicalType::INTEGER)));
    info->columns.push_back(move(ColumnDefinition("query", LogicalType::VARCHAR)));

    auto binder = Binder::CreateBinder(context);
    auto bound_create_info = binder->BindCreateTableInfo(move(info));
    auto &catalog = Catalog::GetCatalog(context);
    catalog.CreateTable(context, bound_create_info.get());
}

void ManageLineage::CreateLineageTables(PhysicalOperator *op, ClientContext &context) {
	string base = op->GetName() + "_" + to_string(query_id) + "_" + to_string( op->id );
    switch (op->type) {
	case PhysicalOperatorType::FILTER: {
		// CREATE TABLE base (value INTEGER, index INTEGER, chunk_id INTEGER)
		auto info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = base;
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());
		CreateLineageTables(op->children[0].get(), context);

		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		// CREATE TABLE base_range (range_start INTEGER, range_end INTEGER, chunk_id INTEGER)
		auto info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = base + "_range";
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		info->columns.push_back(move(ColumnDefinition("range_start", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("range_end", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());

		// CREATE TABLE base_filter (value INTEGER, index INTEGER, chunk_id INTEGER)
		info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = base + "_filter";
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
		bound_create_info = binder->BindCreateTableInfo(move(info));
		catalog.CreateTable(context, bound_create_info.get());

		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		CreateLineageTables(op->children[0].get(), context);
		break;
	}
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
	case PhysicalOperatorType::HASH_GROUP_BY: {
		// CREATE TABLE base_out (value INTEGER, index INTEGER, chunk_id INTEGER)
		auto info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = base + "_OUT";
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());

		// CREATE TABLE base_sink (value INTEGER, index INTEGER, chunk_id INTEGER)
		info = make_unique<CreateTableInfo>();
		info->schema = DEFAULT_SCHEMA;
		info->table = base + "_SINK";
		info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
		info->temporary = false;
		info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
		info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
		bound_create_info = binder->BindCreateTableInfo(move(info));
		catalog.CreateTable(context, bound_create_info.get());

		CreateLineageTables(op->children[0].get(), context);
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
        CreateLineageTables(op->children[0].get(), context);
        break;
	}
    case PhysicalOperatorType::INDEX_JOIN: {
        // CREATE TABLE base_LHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_LHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        // CREATE TABLE base_RHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_RHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get(), context);
        CreateLineageTables(op->children[1].get(), context);
        break;
    }
    case PhysicalOperatorType::HASH_JOIN: {
        // CREATE TABLE base_LHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_LHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        // CREATE TABLE base_RHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_RHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        // CREATE TABLE base_sink (value INTEGER, index INTEGER, chunk_id INTEGER)
        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_SINK";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get(), context);
        CreateLineageTables(op->children[1].get(), context);
        break;
    }
    case PhysicalOperatorType::CROSS_PRODUCT: {
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base;
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("LHS_offset", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("RHS_offset", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get(), context);
        CreateLineageTables(op->children[1].get(), context);
        break;
    }
    case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
    case PhysicalOperatorType::NESTED_LOOP_JOIN: {
        // CREATE TABLE base_LHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_LHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        // CREATE TABLE base_RHS (value INTEGER, index INTEGER, chunk_id INTEGER)
        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = base +  "_RHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());


        CreateLineageTables(op->children[0].get(), context);
        CreateLineageTables(op->children[1].get(), context);
        break;
    }
    case PhysicalOperatorType::LIMIT: {
        // when persisting with limit, we can optimize our code by pushing the limit to childrens
        CreateLineageTables(op->children[0].get(), context);
        break;
    }
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}


void ManageLineage::ForwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int idx, ClientContext &context) {
    std::cout << "ForwardLineage: TraverseTree op " << op << " " << op->GetName() << std::endl;
    switch (op->type) {
    case PhysicalOperatorType::FILTER: { // O(1)
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for filter" << std::endl;
            return;
        }
        // schema: [oidx idx_t, idx idx_t]
        //         maps a row in the output to input index
        vector<idx_t> matches;
        dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).getAllMatches(idx, matches);
        if (matches.size() == 0) {
            std::cout << idx << " index not found " << std::endl;
            return;
        }
        std::cout << idx << " maps to " << matches[0] << std::endl;
        ForwardLineage(op->children[0].get(), move(lineage), matches[0], context);
        break;
    }
    case PhysicalOperatorType::TABLE_SCAN: {
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for table scan" << std::endl;
            return;
        }
        std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
        auto start = dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).start;
        auto end = dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).end;
        if (idx < start || idx > end) return;
        std::cout << "Table scan chunk range " << start << " " << end << std::endl;
        if (collection->collection.find("filter") != collection->collection.end()) {
            std::cout << "filter on scan" << std::endl;
            auto fidx = dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).getAtIndex(idx);
            std::cout << idx << " maps to " << fidx << std::endl;
        } else {
            std::cout << idx << " maps to itself." << std::endl;
        }
        break;
    }
    case PhysicalOperatorType::PROJECTION:
        break;
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for   aggregate" << std::endl;
            return;
        }
        // map index -> group
        idx_t group = 0;
        if (pipelines_lineage[1].find(op) != pipelines_lineage[1].end()) {
            vector<shared_ptr<LineageContext>> sink_lineage = pipelines_lineage[1][op];
            for (idx_t i = 0; i < sink_lineage.size(); ++i) {
                LineageOpUnary *sink_lop = dynamic_cast<LineageOpUnary *>(sink_lineage[i]->GetLineageOp(op, 1).get());
                if (!sink_lop) {
                    std::cout << "something is wrong, aggregate sink lop not found" << std::endl;
                    return;
                }
                group =  dynamic_cast<LineageSelVec&>(*sink_lop->data).getAtIndex(idx);
                break;

            }
        }
        // now that I have the group, I can look for it in lop
        idx_t oidx =  dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).findIndexOf(group);
        std::cout << "Group By " << idx << " belong to " << group << " maps to " << oidx << std::endl;
        break;
    }
    case PhysicalOperatorType::INDEX_JOIN:
    case PhysicalOperatorType::HASH_JOIN:
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}

void ManageLineage::BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx, ClientContext &context) {

    // operator is a sink, build a pipeline
    std::cout << "Backward Lineage: TraverseTree op " << op << " " << op->GetName() << std::endl;
    switch (op->type) {
    case PhysicalOperatorType::FILTER: { // O(1)
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for filter" << std::endl;
            return;
        }
        // schema: [oidx idx_t, idx idx_t]
        //         maps a row in the output to input index
        string tablename = op->GetName() + "_" + to_string( op->id ) ; // + node ID
        idx_t idx = dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).getAtIndex(oidx);
        std::cout << oidx << " maps to " << idx << std::endl;
        BackwardLineage(op->children[0].get(), move(lineage), idx, context);
        break;
    }
    case PhysicalOperatorType::TABLE_SCAN: {
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for  table scan" << std::endl;
            return;
        }
        std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
        // need to adjust the offset based on start
        auto start = dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).start;
        auto end = dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).end;
        std::cout << "Table scan chunk range " << start << " " << end << std::endl;

        if (collection->collection.find("filter") != collection->collection.end()) {
            // get selection vector
            std::cout << "filter on scan" <<  std::endl;
            auto fidx = dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).getAtIndex(oidx);
            std::cout << oidx << " maps to " << fidx << " + offset " << fidx + start << std::endl;
        } else {
            std::cout << oidx << " maps to itself + offset = " << oidx + start << std::endl;
        }
        break;
    }
    case PhysicalOperatorType::PROJECTION: {
        BackwardLineage(op->children[0].get(), move(lineage), oidx, context);
        break;
    }
    case PhysicalOperatorType::SIMPLE_AGGREGATE:
        break;
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
        std::shared_ptr<LineageOpUnary> lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 0));
        if (!lop) {
            std::cout << "something is wrong, lop not found for   aggregate" << std::endl;
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
                std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(sink_lineage[i]->GetLineageOp(op, 1));

                if (!sink_lop) {
                    std::cout << "something is wrong,   aggregate sink lop not found" << std::endl;
                    continue;
                }

                // schema: [ridx idx_t, group idx_t]
                //         maps input row to a specific group
                // getAllMatches: get all ridx that belong to group, O(n)
                dynamic_cast<LineageSelVec&>(*sink_lop->data).getAllMatches(group, matches);
                for (int j =0; j < matches.size(); ++j) {
                    std::cout << " getAllMatches " << matches[j] << std::endl;
                }
            }
        }
        BackwardLineage(op->children[0].get(), move(lineage), oidx, context);
        break;
    }
    case PhysicalOperatorType::TOP_N:
        // single operator, set as child
        // pipeline->child = op->children[0].get();
        break;
    case PhysicalOperatorType::INDEX_JOIN: {
        std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op, 0));

        if (!lop) {
            std::cout << "something is wrong, lop not found" << std::endl;
            return;
        }

        auto lhs_idx = dynamic_cast<LineageSelVec&>(*lop->data_lhs).getAtIndex(oidx);
        std::cout << "-> Index Join LHS " <<  lhs_idx << std::endl;
        auto rhs_idx =  dynamic_cast<LineageDataVector<row_t>&>(*lop->data_rhs).getAtIndex(oidx);
        std::cout << "-> Index Join RHS " <<  rhs_idx << std::endl;

        BackwardLineage(op->children[0].get(), lineage, oidx, context);
        BackwardLineage(op->children[1].get(), lineage, oidx, context);

        break;
    }
    case PhysicalOperatorType::HASH_JOIN: {
        std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op, 0));

        if (!lop) {
            std::cout << "something is wrong, hash join build lop not found" << std::endl;
            return;
        }

        // schema: [oidx idx_t, lhs_idx idx_t]
        //         maps output row to row from probe side (LHS)
        auto lhs_idx =  dynamic_cast<LineageSelVec&>(*lop->data_lhs).getAtIndex(oidx);
        std::cout << "-> Hash Join LHS " <<  lhs_idx << std::endl;

        // schema: [oidx idx_t, rhs_ptr uintptr_t]
        //         maps output row to row from the build side in the hash table payload
        uintptr_t rhs_ptr = dynamic_cast<LineageDataArray<uintptr_t>&>(*lop->data_rhs).getAtIndex(oidx);
        std::cout << "-> Hash Join RHS ptr in HashJoin table " << rhs_ptr<< std::endl;

        // We need to get the actual row id from the build side
        if (pipelines_lineage[1].find(op) != pipelines_lineage[1].end()) {
            vector<shared_ptr<LineageContext>> sink_lineage = pipelines_lineage[1][op];
            for (idx_t i = 0; i < sink_lineage.size(); ++i) {
                std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(sink_lineage[i]->GetLineageOp(op, 1));

                if (!sink_lop) {
                    std::cout << "something is wrong, hash join sink lop not found" << std::endl;
                    continue;
                }

                idx_t rhs_idx = dynamic_cast<LineageDataArray<uintptr_t>&>(*sink_lop->data).findIndexOf((idx_t)rhs_ptr);
                std::cout << "rhs_idx " << i << " " << rhs_idx << " " << rhs_ptr << std::endl;
            }

        }

        BackwardLineage(op->children[0].get(), lineage, oidx, context);
        BackwardLineage(op->children[1].get(), lineage, oidx, context);

        break;
    }
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}


void ManageLineage::Persist(PhysicalOperator *op, shared_ptr<LineageContext> lineage, ClientContext &context, bool is_sink = false) {
    // operator is a sink, build a pipeline
    string tablename = op->GetName() + "_" + to_string(query_id) + "_" + to_string( op->id );

    switch (op->type) {

    case PhysicalOperatorType::LIMIT: { // O(1)
        Persist(op->children[0].get(), move(lineage), context, is_sink);
        break;
	}
    case PhysicalOperatorType::FILTER: { // O(1)
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
             return;
        }
        // schema: [oidx idx_t, idx idx_t]
        //         maps a row in the output to input index
        lop->data->persist(context, tablename, lineage->chunk_id);
        Persist(op->children[0].get(), move(lineage), context, is_sink);
        break;
    }
    case PhysicalOperatorType::TABLE_SCAN: {
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for  table scan" << std::endl;
            return;
        }
        std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
        dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).persist(context, tablename + "_range", lineage->chunk_id);

        if (collection->collection.find("filter") != collection->collection.end()) {
            dynamic_cast<LineageSelVec&>(*collection->collection["filter"]).persist(context, tablename + "_filter" , lineage->chunk_id);

        }
        break;
    }
    case PhysicalOperatorType::PROJECTION: {
        Persist(op->children[0].get(), move(lineage), context);
        break;
    }
    case PhysicalOperatorType::SIMPLE_AGGREGATE:
        Persist(op->children[0].get(), move(lineage), context);
        break;
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
        if (is_sink) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 1));
            if (!sink_lop) {
                return;
            }

            // schema: [ridx idx_t, group idx_t]
            //         maps input row to a specific group
            sink_lop->data->persist(context, tablename+ "_SINK", lineage->chunk_id);
        } else {
            std::shared_ptr<LineageOpUnary> lop =
                std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 0));
            if (!lop) {
                return;
            }

            // schema: [oidx idx_t, group idx_t]
            //         maps a row in the output to a group
            lop->data->persist(context, tablename + "_OUT", lineage->chunk_id);
        }
        break;
    }
    case PhysicalOperatorType::TOP_N:
        // single operator, set as child
        // pipeline->child = op->children[0].get();
        break;
    case PhysicalOperatorType::INDEX_JOIN: {
        std::shared_ptr<LineageOpBinary> lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op, 0));

        if (!lop) {
            return;
        }

        lop->data_lhs->persist(context, tablename+ "_LHS", lineage->chunk_id);
        lop->data_rhs->persist(context, tablename+ "_RHS", lineage->chunk_id);

        Persist(op->children[0].get(), lineage, context, is_sink);
        break;
    }
    case PhysicalOperatorType::HASH_JOIN: {
        if (is_sink) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 1));
            if (!sink_lop) {
                return;
            }
            sink_lop->data->persist(context, tablename+ "_SINK", lineage->chunk_id);
        } else {
            std::shared_ptr<LineageOpCollection> probe_lop =
                std::dynamic_pointer_cast<LineageOpCollection>(lineage->GetLineageOp(op, 0));

            if (!probe_lop) {
                return;
            }

            // schema: [oidx idx_t, lhs_idx idx_t]
            //         maps output row to row from probe side (LHS)

            // schema: [oidx idx_t, rhs_ptr uintptr_t]
            //         maps output row to row from the build side in the hash table payload
            auto lhs_count = 0;
            auto rhs_count = 0;
            for (int i = 0; i < probe_lop->op.size(); ++i) {
                auto op = dynamic_cast<LineageOpBinary&>(*probe_lop->op[i]);
                op.data_lhs->persist(context, tablename + "_LHS", lineage->chunk_id, lhs_count);
                op.data_rhs->persist(context, tablename + "_RHS", lineage->chunk_id, rhs_count);

                lhs_count += dynamic_cast<LineageSelVec&>(*op.data_lhs).count;
                rhs_count += dynamic_cast<LineageDataArray<uintptr_t>&>(*op.data_rhs).count;
            }

            Persist(op->children[0].get(), lineage, context, is_sink);
            Persist(op->children[1].get(), lineage, context, is_sink);
        }
        break;
    }
    case PhysicalOperatorType::CROSS_PRODUCT: {
        if (is_sink) {
            // todo: how to persist right hand side lineage? do we need to?
        } else {
            std::shared_ptr<LineageOpUnary> lop =
                std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 0));

            if (!lop) {
                return;
            }

            // schema: [oidx idx_t, lhs_idx idx_t]
            //         maps output row to row from probe side (LHS)

            lop->data->persist(context, tablename, lineage->chunk_id, 0);
        }
        break;
    }
    case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
    case PhysicalOperatorType::NESTED_LOOP_JOIN: {
        if (is_sink) {
            // todo: how to persist right hand side lineage? do we need to?
        } else {
            std::shared_ptr<LineageOpBinary> lop =
                std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op, 0));

            if (!lop) {
                return;
            }

            // schema: [oidx idx_t, lhs_idx idx_t]
            //         maps output row to row from probe side (LHS)

            // schema: [oidx idx_t, rhs_ptr uintptr_t]
            //         maps output row to row from the build side in the hash table payload
            lop->data_lhs->persist(context, tablename + "_LHS", lineage->chunk_id, 0);
            // add an offset for the right side?
            lop->data_rhs->persist(context, tablename + "_RHS", lineage->chunk_id, 0);
        }
        break;
    }
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}

void ManageLineage::LineageSize() {
    /* unsigned long size = 0;
     for (const auto& elm : pipelines_lineage[0]) {
         std::unordered_map<PhysicalOperator*, unsigned long> size_per_op;
         for (int i=0; i < elm.second.size(); ++i) {
             if (elm.second[i]) {
                 size += elm.second[i]->size_per_op(size_per_op);
             }
         }
         std::cout << "iterate over chunk pipeline: " << std::endl;
         for (const auto& elm_sink: size_per_op) {
             std::cout << elm_sink.first->ToString() << "\n size of: " << elm_sink.second << std::endl;
         }
     }

     std::cout << "pipelines_lineage: " << pipelines_lineage[0].size() << " " << size << std::endl;
     size = 0;
     for (const auto& elm : pipelines_lineage[1]) {
         std::unordered_map<PhysicalOperator*, unsigned long> size_per_op;
         for (int i=0; i < elm.second.size(); ++i) {
             if (elm.second[i]) {
                 size += elm.second[i]->size_per_op(size_per_op);
             }
         }
         std::cout << "iterate over sink pipeline: " << std::endl;
         for (const auto& elm_sink: size_per_op) {
             std::cout << elm_sink.first->ToString()  << "\n size of: " << elm_sink.second << std::endl;
         }
     }

     std::cout << "since_lineage: " << pipelines_lineage[1].size() << " " << size << std::endl;*/
}

} // namespace duckdb
