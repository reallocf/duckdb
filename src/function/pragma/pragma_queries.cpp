#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/index/lineage_index/lineage_index.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/scan/physical_empty_result.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/set/physical_union.hpp"
#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"

namespace duckdb {

#ifdef LINEAGE
unique_ptr<PhysicalIndexJoin> PreparePhysicalIndexJoin(PhysicalOperator *op, unique_ptr<PhysicalOperator> left, ClientContext &cxt, ChunkCollection *chunk_collection) {
	auto logical_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
	logical_join->types = {LogicalType::UBIGINT};
	vector<JoinCondition> conds;
	unique_ptr<Expression> left_expression = make_unique<BoundReferenceExpression>("", LogicalType::UBIGINT, 0);
	unique_ptr<Expression> right_expression = make_unique<BoundReferenceExpression>("", LogicalType::UBIGINT, 0);
	JoinCondition cond = JoinCondition();
	cond.left = move(left_expression);
	cond.right = move(right_expression);
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	cond.null_values_are_equal = false;
	conds.push_back(move(cond));
	vector<LogicalType> empty_types = {LogicalType::UBIGINT};
	const vector<column_t> cids = {0L};
	unique_ptr<ColumnBinding> col_bind = make_unique<ColumnBinding>(0,0);
	unique_ptr<BoundColumnRefExpression> exp = make_unique<BoundColumnRefExpression>(LogicalType::UBIGINT, *col_bind.get());
	vector<unique_ptr<Expression>> exps;
	exps.push_back(move(exp));
	unique_ptr<Index> lineage_index = make_unique<LineageIndex>(cids, exps, op->lineage_op.at(-1));
	lineage_index->unbound_expressions.push_back(move(exp));
	lineage_index->column_ids.push_back(0);
	vector<idx_t> left_projection_map = {};
	vector<idx_t> right_projection_map = {0};
	vector<column_t> column_ids = {0};
	unique_ptr<PhysicalOperator> right = make_unique<PhysicalEmptyResult>(empty_types, 1);
	return make_unique<PhysicalIndexJoin>(
	    *logical_join.get(),
	    move(left),
	    move(right),
	    move(conds),
	    JoinType::INNER,
	    left_projection_map,
	    right_projection_map,
	    column_ids,
	    lineage_index.get(),
	    false,
	    1, // TODO improve this?
	    true,
	    op->lineage_op.at(-1),
	    chunk_collection
	);
}


unique_ptr<PhysicalOperator> GenerateCustomPlan(
    PhysicalOperator* op,
    ClientContext &cxt,
    int lineage_id,
	unique_ptr<PhysicalOperator> left,
    bool simple_agg_flag,
	vector<unique_ptr<PhysicalOperator>> *pipelines
) {
	if (!op) {
		return left;
	}
	if (op->type == PhysicalOperatorType::HASH_JOIN && dynamic_cast<PhysicalJoin *>(op)->join_type == JoinType::MARK) {
		// Skip Mark Joins
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
	}
	if (op->type == PhysicalOperatorType::PROJECTION) {
		// Skip Projections
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
	}
	if (op->type == PhysicalOperatorType::DELIM_SCAN) {
		// Skip DELIM_SCANs since they never affect the lineage
		return left;
	}
	if (op->children.empty()) {
		return PreparePhysicalIndexJoin(op, move(left), cxt, nullptr);
	}
	if (simple_agg_flag && (
		op->type == PhysicalOperatorType::HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::PERFECT_HASH_GROUP_BY ||
		op->type == PhysicalOperatorType::SIMPLE_AGGREGATE ||
		op->type == PhysicalOperatorType::ORDER_BY ||
		op->type == PhysicalOperatorType::PROJECTION
	)) {
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), true, pipelines);
	}
	vector<LogicalType> types = {LogicalType::UBIGINT};
	PhysicalOperatorType op_type = PhysicalOperatorType::CHUNK_SCAN;
	idx_t estimated_cardinality = 1;

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		if (!op->delim_handled) {
			// TODO handle multithreading here?
			idx_t thread_id = -1;

			// set this child to join's child to appropriately line up chunk scan lineage
			dynamic_cast<PhysicalDelimJoin *>(op)->join->children[0] = move(op->children[0]);

			// distinct input is delim join input
			// distinct should be the input to delim scan
			op->lineage_op[thread_id]->children[2]->children.push_back(op->lineage_op[thread_id]->children[0]);

			// chunk scan input is delim join input
			op->lineage_op[thread_id]->children[1]->children[1] = op->lineage_op[thread_id]->children[0];

			// we only want to do the child reordering once
			op->delim_handled = true;
		}
		return GenerateCustomPlan(dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), cxt, lineage_id, move(left), simple_agg_flag, pipelines);
	}
	if (left == nullptr) {
		unique_ptr<PhysicalChunkScan> chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
		DataChunk lineage_chunk;
		lineage_chunk.Initialize(types);
		lineage_chunk.SetCardinality(1);
		Vector lineage(Value::UBIGINT(lineage_id));
		lineage_chunk.data[0].Reference(lineage);
		chunk_scan->collection = new ChunkCollection();
		chunk_scan->collection->Append(lineage_chunk);
		if (
		    op->children.size() == 2 &&
		    (op->type == PhysicalOperatorType::INDEX_JOIN || op->type == PhysicalOperatorType::CROSS_PRODUCT || dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI)
		) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, build_chunk_scan->collection), false, pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines));
			}

			return custom_plan;
		}
		return GenerateCustomPlan(
		    op->children[0].get(),
		    cxt,
		    lineage_id,
		    PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, nullptr),
		    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
		    pipelines
		);
	} else {
		if (op->children.size() == 2 && dynamic_cast<PhysicalJoin *>(op)->join_type != JoinType::ANTI) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(left), cxt,  build_chunk_scan->collection), false, pipelines);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines->push_back(move(build_chunk_scan));
			} else {
				pipelines->push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false, pipelines));
			}

			// probe side of hash join
			return custom_plan;
		}
		return GenerateCustomPlan(
		    op->children[0].get(),
		    cxt,
		    lineage_id,
		    PreparePhysicalIndexJoin(op, move(left), cxt, nullptr),
		    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE,
		    pipelines
		);
	}
}

unique_ptr<PhysicalOperator> CombineByMode(
	ClientContext &context,
	const string& mode,
    bool should_count,
    unique_ptr<PhysicalOperator> first_plan,
    vector<unique_ptr<PhysicalOperator>> other_plans
) {
	unique_ptr<PhysicalOperator> final_plan = move(first_plan);
	if (mode == "LIN") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		// Count of Lineage - union then aggregate
		for (idx_t i = 0; i < other_plans.size(); i++) {
			final_plan = make_unique<PhysicalUnion>(
				types,
				move(final_plan),
				move(other_plans[i]),
			    1 // TODO improve this?
			);
		}
	} else if (mode == "PERM") {
		vector<LogicalType> types = {LogicalType::UBIGINT};
		for (idx_t i = 0; i < other_plans.size(); i++) {
			types.push_back(LogicalType::UBIGINT);
			final_plan = make_unique<PhysicalCrossProduct>(
			    types,
			    move(other_plans[i]),
				move(final_plan),
			    1 // TODO improve this?
			);
		}
	} else {
		// Invalid mode
		throw std::logic_error("Invalid backward query mode - should one of [LIN, PERM, PROV]");
	}
	if (should_count) {
		// Add final aggregation at end
		vector<LogicalType> types = {LogicalType::UBIGINT};
		vector<unique_ptr<Expression>> agg_exprs;

		agg_exprs.push_back(AggregateFunction::BindAggregateFunction(context, CountStarFun::GetFunction(), {}));
		unique_ptr<PhysicalOperator> agg = make_unique<PhysicalSimpleAggregate>(types, move(agg_exprs), true, 1);

		agg->children.push_back(move(final_plan));
		final_plan = move(agg);
	}
	return final_plan;
}

template <typename T>
void Reverse(vector<unique_ptr<T>> *vec) {
	for (idx_t i = 0; i < vec->size() / 2; i++) {
		unique_ptr<T> tmp = move(vec->at(i));
		vec->at(i) = move(vec->at(vec->size() - i - 1));
		vec->at(vec->size() - i - 1) = move(tmp);
	}
}

string PragmaBackwardLineageDuckDBExecEngine(ClientContext &context, const FunctionParameters &parameters) {
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	int lineage_id = parameters.values[1].GetValue<int>();
	string mode = parameters.values[2].ToString();
	bool should_count = parameters.values[3].GetValue<int>() != 0;
	auto op = context.query_to_plan[query].get();
	if (op == nullptr) {
		throw std::logic_error("Querying non-existent lineage");
	}
	clock_t start = clock();
	vector<unique_ptr<PhysicalOperator>> other_plans;
	unique_ptr<PhysicalOperator> first_plan = GenerateCustomPlan(op, context, lineage_id, nullptr, false, &other_plans);
	clock_t plan = clock();
	// We construct other_plans in reverse execution order, swap here
	Reverse(&other_plans);

	unique_ptr<PhysicalOperator> final_plan = CombineByMode(context, mode, should_count, move(first_plan), move(other_plans));
	unique_ptr<QueryResult> result = context.RunPlan(final_plan.get());
	clock_t execute = clock();

	string str_results;
	idx_t count = 0;
	unique_ptr<DataChunk> chunk = result->Fetch();
	while (chunk != nullptr) {
		for (idx_t row_idx = 0; row_idx < chunk->size(); row_idx++) {
			count++;
			string row;
			for (idx_t col_idx = 0; col_idx < chunk->ColumnCount(); col_idx++) {
				int val = chunk->GetValue(col_idx, row_idx).GetValue<int>();

				if (!row.empty()) {
					row += StringUtil::Format(", %d", val);
				} else {
					row += to_string(val);
				}
			}
			if (chunk->ColumnCount() > 1) {
				row = "list_value(" + row + ")";
			}
			if (!str_results.empty()) {
				str_results += ", " + row;
			} else {
				str_results += row;
			}
		}
		chunk = result->Fetch();
	}
	if (count > 1) {
		str_results = "list_value(" + str_results + ")";
	}
	clock_t end = clock();
	std::cout << "Plan time: " << ((float) plan - start) / CLOCKS_PER_SEC << std::endl;
	std::cout << "Execute time: " << ((float) execute - plan) / CLOCKS_PER_SEC << std::endl;
	std::cout << "List build time: " << ((float) end - execute) / CLOCKS_PER_SEC << std::endl;
	std::cout << "Total time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
	return StringUtil::Format("SELECT %s", str_results);
}
#endif

string PragmaTableInfo(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM pragma_table_info('%s')", parameters.values[0].ToString());
}

string PragmaShowTables(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT name FROM sqlite_master ORDER BY name";
}

string PragmaAllProfiling(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_last_profiling_output() JOIN pragma_detailed_profiling_output() ON "
	       "(pragma_last_profiling_output.operator_id);";
}

string PragmaDatabaseList(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_database_list() ORDER BY 1";
}

string PragmaCollations(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_collations() ORDER BY 1";
}

string PragmaFunctionsQuery(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_functions() ORDER BY 1";
}

string PragmaShow(ClientContext &context, const FunctionParameters &parameters) {
	// PRAGMA table_info but with some aliases
	return StringUtil::Format(
	    "SELECT name AS \"Field\", type as \"Type\", CASE WHEN \"notnull\" THEN 'NO' ELSE 'YES' END AS \"Null\", "
	    "NULL AS \"Key\", dflt_value AS \"Default\", NULL AS \"Extra\" FROM pragma_table_info('%s')",
	    parameters.values[0].ToString());
}

string PragmaVersion(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_version()";
}

string PragmaImportDatabase(ClientContext &context, const FunctionParameters &parameters) {
	auto &fs = FileSystem::GetFileSystem(context);
	string query;
	// read the "shema.sql" and "load.sql" files
	vector<string> files = {"schema.sql", "load.sql"};
	for (auto &file : files) {
		auto file_path = fs.JoinPath(parameters.values[0].ToString(), file);
		auto handle = fs.OpenFile(file_path, FileFlags::FILE_FLAGS_READ);
		auto fsize = fs.GetFileSize(*handle);
		auto buffer = unique_ptr<char[]>(new char[fsize]);
		fs.Read(*handle, buffer.get(), fsize);

		query += string(buffer.get(), fsize);
	}
	return query;
}

string PragmaDatabaseSize(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM pragma_database_size()";
}

string PragmaStorageInfo(ClientContext &context, const FunctionParameters &parameters) {
	return StringUtil::Format("SELECT * FROM pragma_storage_info('%s')", parameters.values[0].ToString());
}

void PragmaQueries::RegisterFunction(BuiltinFunctions &set) {
#ifdef LINEAGE
	set.AddFunction(PragmaFunction::PragmaCall("lineage_query", PragmaBackwardLineageDuckDBExecEngine, {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR, LogicalType::INTEGER}));
#endif
	set.AddFunction(PragmaFunction::PragmaCall("table_info", PragmaTableInfo, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("storage_info", PragmaStorageInfo, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("show_tables", PragmaShowTables));
	set.AddFunction(PragmaFunction::PragmaStatement("database_list", PragmaDatabaseList));
	set.AddFunction(PragmaFunction::PragmaStatement("collations", PragmaCollations));
	set.AddFunction(PragmaFunction::PragmaCall("show", PragmaShow, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("version", PragmaVersion));
	set.AddFunction(PragmaFunction::PragmaStatement("database_size", PragmaDatabaseSize));
	set.AddFunction(PragmaFunction::PragmaStatement("functions", PragmaFunctionsQuery));
	set.AddFunction(PragmaFunction::PragmaCall("import_database", PragmaImportDatabase, {LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaStatement("all_profiling_output", PragmaAllProfiling));
}

} // namespace duckdb
