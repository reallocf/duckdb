#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/task_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include <algorithm>

namespace duckdb {

Executor::Executor(ClientContext &context) : context(context) {
}

Executor::~Executor() {
}

void Executor::Initialize(PhysicalOperator *plan) {
	Reset();

	physical_plan = plan;
	physical_state = physical_plan->GetOperatorState();

	context.profiler.Initialize(physical_plan);
	auto &scheduler = TaskScheduler::GetScheduler(context);
	this->producer = scheduler.CreateProducer();

	BuildPipelines(physical_plan, nullptr);

	this->total_pipelines = pipelines.size();

	// schedule pipelines that do not have dependents
	for (auto &pipeline : pipelines) {
		if (!pipeline->HasDependencies()) {
			pipeline->Schedule();
		}
	}

	// now execute tasks from this producer until all pipelines are completed
	while (completed_pipelines < total_pipelines) {
		unique_ptr<Task> task;
		while (scheduler.GetTaskFromProducer(*producer, task)) {
			task->Execute();
			task.reset();
		}
	}

	pipelines.clear();
	if (!exceptions.empty()) {
		// an exception has occurred executing one of the pipelines
		throw Exception(exceptions[0]);
	}
}

void Executor::LineageSize() {
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

void Executor::Reset() {
    chunk_id = 0;
	pipelines_lineage.clear();
    delim_join_dependencies.clear();
	recursive_cte = nullptr;
	physical_plan = nullptr;
	physical_state = nullptr;
	completed_pipelines = 0;
	total_pipelines = 0;
	exceptions.clear();
	pipelines.clear();
}

void Executor::BuildPipelines(PhysicalOperator *op, Pipeline *parent) {
	if (op->IsSink()) {
		// operator is a sink, build a pipeline
		auto pipeline = make_unique<Pipeline>(*this, *producer);
		pipeline->sink = (PhysicalSink *)op;
		pipeline->sink_state = pipeline->sink->GetGlobalState(context);
		if (parent) {
			// the parent is dependent on this pipeline to complete
			parent->AddDependency(pipeline.get());
		}
		switch (op->type) {
		case PhysicalOperatorType::CREATE_TABLE_AS:
		case PhysicalOperatorType::INSERT:
		case PhysicalOperatorType::DELETE_OPERATOR:
		case PhysicalOperatorType::UPDATE:
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::SIMPLE_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		case PhysicalOperatorType::WINDOW:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::RESERVOIR_SAMPLE:
		case PhysicalOperatorType::TOP_N:
		case PhysicalOperatorType::COPY_TO_FILE:
			// single operator, set as child
			pipeline->child = op->children[0].get();
			break;
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
			// regular join, create a pipeline with RHS source that sinks into this pipeline
			pipeline->child = op->children[1].get();
			// on the LHS (probe child), we recurse with the current set of pipelines
			BuildPipelines(op->children[0].get(), parent);
			break;
		case PhysicalOperatorType::DELIM_JOIN: {
			// duplicate eliminated join
			// create a pipeline with the duplicate eliminated path as source
			pipeline->child = op->children[0].get();
			break;
		}
		default:
			throw InternalException("Unimplemented sink type!");
		}
		// recurse into the pipeline child
		BuildPipelines(pipeline->child, pipeline.get());
		for (auto &dependency : pipeline->GetDependencies()) {
			auto dependency_cte = dependency->GetRecursiveCTE();
			if (dependency_cte) {
				pipeline->SetRecursiveCTE(dependency_cte);
			}
		}
		if (op->type == PhysicalOperatorType::DELIM_JOIN) {
			// for delim joins, recurse into the actual join
			// any pipelines in there depend on the main pipeline
			auto &delim_join = (PhysicalDelimJoin &)*op;
			// any scan of the duplicate eliminated data on the RHS depends on this pipeline
			// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
			for (auto &delim_scan : delim_join.delim_scans) {
				delim_join_dependencies[delim_scan] = pipeline.get();
			}
			BuildPipelines(delim_join.join.get(), parent);
		}
		auto pipeline_cte = pipeline->GetRecursiveCTE();
		if (!pipeline_cte) {
			// regular pipeline: schedule it
			pipelines.push_back(move(pipeline));
		} else {
			// add it to the set of dependent pipelines in the CTE
			auto &cte = (PhysicalRecursiveCTE &)*pipeline_cte;
			cte.pipelines.push_back(move(pipeline));
		}
	} else {
		// operator is not a sink! recurse in children
		// first check if there is any additional action we need to do depending on the type
		switch (op->type) {
		case PhysicalOperatorType::DELIM_SCAN: {
			auto entry = delim_join_dependencies.find(op);
			D_ASSERT(entry != delim_join_dependencies.end());
			// this chunk scan introduces a dependency to the current pipeline
			// namely a dependency on the duplicate elimination pipeline to finish
			D_ASSERT(parent);
			parent->AddDependency(entry->second);
			break;
		}
		case PhysicalOperatorType::EXECUTE: {
			// EXECUTE statement: build pipeline on child
			auto &execute = (PhysicalExecute &)*op;
			BuildPipelines(execute.plan, parent);
			break;
		}
		case PhysicalOperatorType::RECURSIVE_CTE: {
			auto &cte_node = (PhysicalRecursiveCTE &)*op;
			// recursive CTE: we build pipelines on the LHS as normal
			BuildPipelines(op->children[0].get(), parent);
			// for the RHS, we gather all pipelines that depend on the recursive cte
			// these pipelines need to be rerun
			if (recursive_cte) {
				throw InternalException("Recursive CTE detected WITHIN a recursive CTE node");
			}
			recursive_cte = op;
			BuildPipelines(op->children[1].get(), parent);
			// re-order the pipelines such that they are executed in the correct order of dependencies
			for (idx_t i = 0; i < cte_node.pipelines.size(); i++) {
				auto &deps = cte_node.pipelines[i]->GetDependencies();
				for (idx_t j = i + 1; j < cte_node.pipelines.size(); j++) {
					if (deps.find(cte_node.pipelines[j].get()) != deps.end()) {
						// pipeline "i" depends on pipeline "j" but pipeline "i" is scheduled to be executed before
						// pipeline "j"
						std::swap(cte_node.pipelines[i], cte_node.pipelines[j]);
						i--;
						continue;
					}
				}
			}
			for (idx_t i = 0; i < cte_node.pipelines.size(); i++) {
				cte_node.pipelines[i]->ClearParents();
			}
			if (parent) {
				parent->SetRecursiveCTE(nullptr);
			}

			recursive_cte = nullptr;
			return;
		}
		case PhysicalOperatorType::RECURSIVE_CTE_SCAN: {
			if (!recursive_cte) {
				throw InternalException("Recursive CTE scan found without recursive CTE node");
			}
			if (parent) {
				// found a recursive CTE scan in a child pipeline
				// mark the child pipeline as recursive
				parent->SetRecursiveCTE(recursive_cte);
			}
			break;
		}
		default:
			break;
		}
		for (auto &child : op->children) {
			BuildPipelines(child.get(), parent);
		}
	}
}

vector<LogicalType> Executor::GetTypes() {
	D_ASSERT(physical_plan);
	return physical_plan->GetTypes();
}

void Executor::PushError(const string &exception) {
	lock_guard<mutex> elock(executor_lock);
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
	// push the exception onto the stack
	exceptions.push_back(exception);
}

void Executor::Flush(ThreadContext &tcontext) {
	lock_guard<mutex> elock(executor_lock);
	context.profiler.Flush(tcontext.profiler);
}

bool Executor::GetPipelinesProgress(int &current_progress) {
	if (!pipelines.empty()) {
		return pipelines.back()->GetProgress(current_progress);
	} else {
		current_progress = -1;
		return true;
	}
}

void Executor::ForwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int idx) {
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
        lop->data->getAllMatches(idx, matches);
		if (matches.size() == 0) {
			std::cout << idx << " index not found " << std::endl;
			return;
		}
        std::cout << idx << " maps to " << matches[0] << std::endl;
        BackwardLineage(op->children[0].get(), move(lineage), matches[0]);
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
            auto fidx = dynamic_cast<LineageRange&>(*collection->collection["filter"]).getAtIndex(idx);
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
                group = sink_lop->data->getAtIndex(idx);
				break;

            }
        }
		// now that I have the group, I can look for it in lop
        idx_t oidx = lop->data->findIndexOf(group);
        std::cout << "Group By " << idx << " belong to " << group << " maps to " << oidx << std::endl;
        break;
    }
    case PhysicalOperatorType::INDEX_JOIN:
    case PhysicalOperatorType::HASH_JOIN:
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}

void Executor::AnnotatePlan(PhysicalOperator *op) {
	if (!op) return;
	if ( op_metadata.find(op->GetName()) == op_metadata.end() )
		op_metadata[op->GetName()] =  0;
	else
		op_metadata[op->GetName()]++;
    op->id = op_metadata[op->GetName()];
    std::cout << op->GetName() << " " << op->id << std::endl;

	for (int i = 0; i < op->children.size(); ++i)
		AnnotatePlan(op->children[i].get());
}

void Executor::CreateLineageTables(PhysicalOperator *op) {
    switch (op->type) {
    case PhysicalOperatorType::FILTER: {
        // CREATE TABLE  op->GetName() (input INTEGER, output INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) ;
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());
        break;
	}
    case PhysicalOperatorType::TABLE_SCAN: {
        // CREATE TABLE  op->GetName() (input INTEGER, output INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) + "_range";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("range_start", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("range_end", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());


        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) + "_filter";
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
		CreateLineageTables(op->children[0].get());
		break;
	}
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
        // CREATE TABLE  op->GetName() (input INTEGER, output INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) + "_OUT";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) +  "_SINK";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get());
        break;
	}
    case PhysicalOperatorType::SIMPLE_AGGREGATE:
		break;
    case PhysicalOperatorType::INDEX_JOIN: {
        // CREATE TABLE  op->GetName() (input INTEGER, output INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName()+ "_" + to_string( op->id ) +  "_LHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName()+ "_" + to_string( op->id ) +  "_RHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get());
        CreateLineageTables(op->children[1].get());
		break;
    }
    case PhysicalOperatorType::HASH_JOIN: {
        // CREATE TABLE  op->GetName() (input INTEGER, output INTEGER, chunk_id INTEGER)
        auto info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName()+ "_" + to_string( op->id ) +  "_LHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        auto binder = Binder::CreateBinder(context);
        auto bound_create_info = binder->BindCreateTableInfo(move(info));
        auto &catalog = Catalog::GetCatalog(context);
        catalog.CreateTable(context, bound_create_info.get());

        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName()+ "_" + to_string( op->id ) +  "_RHS";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());


        info = make_unique<CreateTableInfo>();
        info->schema = DEFAULT_SCHEMA;
        info->table = op->GetName() + "_" + to_string( op->id ) +  "_SINK";
        info->on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
        info->temporary = false;
        info->columns.push_back(move(ColumnDefinition("value", LogicalType::BIGINT)));
        info->columns.push_back(move(ColumnDefinition("index", LogicalType::INTEGER)));
        info->columns.push_back(move(ColumnDefinition("chunk_id", LogicalType::INTEGER)));
        bound_create_info = binder->BindCreateTableInfo(move(info));
        catalog.CreateTable(context, bound_create_info.get());

        CreateLineageTables(op->children[0].get());
        CreateLineageTables(op->children[1].get());
        break;
    }
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}

void Executor::BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx) {

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
        idx_t idx = lop->data->getAtIndex(oidx);
		lop->data->persist(context, tablename, lineage->chunk_id);
        std::cout << oidx << " maps to " << idx << std::endl;

        BackwardLineage(op->children[0].get(), move(lineage), idx);
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
        string tablename = op->GetName() + "_" + to_string( op->id ) + "_range" ; // + node ID
        dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).persist(context, tablename, lineage->chunk_id);

        if (collection->collection.find("filter") != collection->collection.end()) {
            // get selection vector
            std::cout << "filter on scan" <<  std::endl;
            auto fidx = dynamic_cast<LineageDataArray<sel_t>&>(*collection->collection["filter"]).getAtIndex(oidx);
            std::cout << oidx << " maps to " << fidx << " + offset " << fidx + start << std::endl;
            string tablename = op->GetName() + "_" + to_string( op->id ) + "_filter" ; // + node ID
            dynamic_cast<LineageDataArray<sel_t>&>(*collection->collection["filter"]).persist(context, tablename, lineage->chunk_id);

        } else {
            std::cout << oidx << " maps to itself + offset = " << oidx + start << std::endl;
        }
        break;
	}
	case PhysicalOperatorType::PROJECTION: {
		BackwardLineage(op->children[0].get(), move(lineage), oidx);
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
        idx_t group = lop->data->getAtIndex(oidx);
        std::cout << oidx << " belong to " << group << std::endl;
        string tablename = op->GetName()  + "_" + to_string( op->id ) +  "_OUT"; // + node ID
        lop->data->persist(context, tablename, lineage->chunk_id);

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
                string tablename = op->GetName() + "_" + to_string( op->id ) + "_SINK"; // + node ID
                sink_lop->data->persist(context, tablename, lineage->chunk_id);


                // schema: [ridx idx_t, group idx_t]
				//         maps input row to a specific group
				// getAllMatches: get all ridx that belong to group, O(n)
                sink_lop->data->getAllMatches(group, matches);
                for (int j =0; j < matches.size(); ++j) {
                    std::cout << " getAllMatches " << matches[j] << std::endl;
                }
            }
        }
        BackwardLineage(op->children[0].get(), move(lineage), oidx);
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

        auto lhs_idx = lop->data_lhs->getAtIndex(oidx);
        std::cout << "-> Index Join LHS " <<  lhs_idx << std::endl;
        auto rhs_idx = lop->data_rhs->getAtIndex(oidx);
        std::cout << "-> Index Join RHS " <<  rhs_idx << std::endl;

        string tablename = op->GetName()+ "_" + to_string( op->id ) ; // + node ID
        lop->data_lhs->persist(context, tablename+ "_LHS", lineage->chunk_id);
        lop->data_rhs->persist(context, tablename+ "_RHS", lineage->chunk_id);

        BackwardLineage(op->children[0].get(), lineage, oidx);
        BackwardLineage(op->children[1].get(), lineage, oidx);

        break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
        std::shared_ptr<LineageOpBinary> build_lop = std::dynamic_pointer_cast<LineageOpBinary>(lineage->GetLineageOp(op, 0));

        if (!build_lop) {
			std::cout << "something is wrong, hash join build lop not found" << std::endl;
			return;
		}

		// schema: [oidx idx_t, lhs_idx idx_t]
		//         maps output row to row from probe side (LHS)
		auto lhs_idx = build_lop->data_lhs->getAtIndex(oidx);
		std::cout << "-> Hash Join LHS " <<  lhs_idx << std::endl;

		// schema: [oidx idx_t, rhs_ptr uintptr_t]
		//         maps output row to row from the build side in the hash table payload
		uintptr_t rhs_ptr = build_lop->data_rhs->getAtIndex(oidx);
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

				idx_t rhs_idx = sink_lop->data->findIndexOf((idx_t)rhs_ptr);
                std::cout << "rhs_idx " << i << " " << rhs_idx << " " << rhs_ptr << std::endl;
            }

		}

		BackwardLineage(op->children[0].get(), lineage, oidx);
        BackwardLineage(op->children[1].get(), lineage, oidx);

        break;
	}
	default:
		std::cout << "Unimplemented op type!" << std::endl;
	}
}


void Executor::Persist(PhysicalOperator *op, shared_ptr<LineageContext> lineage, bool is_sink = false) {
    // operator is a sink, build a pipeline
    std::cout << "Persist: " << op << " " << op->GetName() << " is_sink " << is_sink << std::endl;
    string tablename = op->GetName() + "_" + to_string(op->id); // + node ID
    switch (op->type) {
    case PhysicalOperatorType::FILTER: { // O(1)
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for filter" << std::endl;
            return;
        }
        // schema: [oidx idx_t, idx idx_t]
        //         maps a row in the output to input index
        lop->data->persist(context, tablename, lineage->chunk_id);
        Persist(op->children[0].get(), move(lineage), is_sink);
        break;
    }
    case PhysicalOperatorType::TABLE_SCAN: {
        LineageOpUnary *lop = dynamic_cast<LineageOpUnary *>(lineage->GetLineageOp(op, 0).get());
        if (!lop) {
            std::cout << "something is wrong, lop not found for  table scan" << std::endl;
            return;
        }
        std::shared_ptr<LineageCollection> collection = std::dynamic_pointer_cast<LineageCollection>(lop->data);
        string tablename = op->GetName() + "_" + to_string( op->id ) + "_range" ; // + node ID
        dynamic_cast<LineageRange&>(*collection->collection["rowid_range"]).persist(context, tablename, lineage->chunk_id);

        if (collection->collection.find("filter") != collection->collection.end()) {
            dynamic_cast<LineageDataArray<sel_t>&>(*collection->collection["filter"]).persist(context, tablename + "_filter" , lineage->chunk_id);

        }
        break;
    }
    case PhysicalOperatorType::PROJECTION: {
        Persist(op->children[0].get(), move(lineage));
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
        lop->data->persist(context, tablename +  "_OUT", lineage->chunk_id);
        Persist(op->children[0].get(), move(lineage), is_sink);
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

        lop->data_lhs->persist(context, tablename+ "_LHS", lineage->chunk_id);
        lop->data_rhs->persist(context, tablename+ "_RHS", lineage->chunk_id);

        Persist(op->children[0].get(), lineage, is_sink);
        break;
    }
    case PhysicalOperatorType::HASH_JOIN: {
		if (is_sink) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op, 1));
            if (!sink_lop) {
				std::cout << "something is wrong, hash join build lop not found" << std::endl;
				return;
			}
			std::cout << "found for sink!" << std::endl;
            sink_lop->data->persist(context, tablename+ "_SINK", lineage->chunk_id);
        } else {
			std::shared_ptr<LineageOpCollection> probe_lop =
			    std::dynamic_pointer_cast<LineageOpCollection>(lineage->GetLineageOp(op, 0));

			if (!probe_lop) {
				std::cout << "something is wrong, hash join probe_lop lop not found" << std::endl;
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

				lhs_count += dynamic_cast<LineageDataArray<sel_t>&>(*op.data_lhs).count;
				rhs_count += dynamic_cast<LineageDataArray<uintptr_t>&>(*op.data_rhs).count;
            }

			Persist(op->children[0].get(), lineage, is_sink);
			Persist(op->children[1].get(), lineage, is_sink);
		}
        break;
    }
    default:
        std::cout << "Unimplemented op type!" << std::endl;
    }
}

void Executor::QueryLineage(shared_ptr<LineageContext> lineage) {
    PhysicalOperator* root = physical_plan;
    // I have the physical plan -> use it to compute the lineage
    // how would I know if I should access chunk_lineage or sink lineage?
    if (!lineage)
        return;

    int oidx = 0;
    BackwardLineage(root, lineage, oidx);
	int fidx = 0;
    ForwardLineage(root, lineage, fidx);
}

void Executor::AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext> lineage) {
	if (lineage->isEmpty() == false) {
		// need to associate output chunkid with this
		pipelines_lineage[0][opKey].push_back(move(lineage));
    }
}

void Executor::AddLocalSinkLineage(PhysicalOperator* sink,  shared_ptr<LineageContext> lineage) {
    if (lineage->isEmpty() == false) {
        lock_guard<mutex> elock(executor_lock);
		pipelines_lineage[1][sink].push_back(move(lineage));
    }
}

unique_ptr<DataChunk> Executor::FetchChunk() {
    D_ASSERT(physical_plan);

    ThreadContext thread(context);
    TaskContext task;
    ExecutionContext econtext(context, thread, task);

	std::cout << physical_plan->ToString() << std::endl;
    auto chunk = make_unique<DataChunk>();
    // run the plan to get the next chunks
    physical_plan->InitializeChunkEmpty(*chunk);
    physical_plan->GetChunk(econtext, *chunk, physical_state.get());
    physical_plan->FinalizeOperatorState(*physical_state, econtext);

#ifdef LINEAGE
    // Flush the lineage to global storage location
    if (context.trace_lineage  && econtext.lineage && !econtext.lineage->isEmpty()) {
        econtext.lineage->chunk_id = chunk_id++;
		Persist(physical_plan, econtext.lineage);
        //QueryLineage( econtext.lineage );
        this->AddOutputLineage(physical_plan, move(econtext.lineage));}

#endif

    context.profiler.Flush(thread.profiler);

    return chunk;
}

} // namespace duckdb
