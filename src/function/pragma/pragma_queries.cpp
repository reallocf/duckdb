#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/index/lineage_index/lineage_index.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/scan/physical_empty_result.hpp"


namespace duckdb {

#ifdef LINEAGE
string PragmaBackwardLineage(ClientContext &context, const FunctionParameters &parameters) {
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	string mode = parameters.values[1].ToString();
	auto op = context.query_to_plan[query].get();
	if (op == nullptr) {
		throw std::logic_error("Querying non-existent lineage");
	}

	string origin = parameters.values[2].ToString();
	std::stringstream ss(origin);
	string word;
	if (mode == "VALUE") {
		string out_str;
		while (ss >> word) {
			clock_t start = clock();
			auto lineage = context.lineage_manager->Backward(op, (idx_t)stoi(word));
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
			for (idx_t el : lineage) {
				if (!out_str.empty()) {
					out_str += ",";
				}
				out_str += to_string(el);
			}
		}
		out_str = "list_value("+ out_str +")";

		return StringUtil::Format("SELECT %s", out_str);
	} else if (mode == "COUNT") {
		idx_t out_idx;
		while (ss >> word) {
			clock_t start = clock();
			out_idx = context.lineage_manager->BackwardCount(op, (idx_t)stoi(word), LIN);
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		}

		return StringUtil::Format("SELECT %i", out_idx);
	} else if (mode == "LIN") {
		idx_t out_idx;
		while (ss >> word) {
			clock_t start = clock();
			out_idx = context.lineage_manager->BackwardCount(op, (idx_t)stoi(word), LIN);
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		}

		return StringUtil::Format("SELECT %i", out_idx);
	} else if (mode == "PERM") {
		idx_t out_idx;
		while (ss >> word) {
			clock_t start = clock();
			out_idx = context.lineage_manager->BackwardCount(op, (idx_t)stoi(word), PERM);
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		}

		return StringUtil::Format("SELECT %i", out_idx);
	} else if (mode == "PROV") {
		idx_t out_idx;
		while (ss >> word) {
			clock_t start = clock();
			out_idx = context.lineage_manager->BackwardCount(op, (idx_t)stoi(word), PROV);
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		}

		return StringUtil::Format("SELECT %i", out_idx);
	} else {
		throw std::logic_error("Invalid backward query mode - should one of [VALUE, COUNT, LIN, PERM, PROV]");
	}
}
#endif


std::unique_ptr<PhysicalIndexJoin> PreparePhysicalIndexJoin(PhysicalOperator *op, unique_ptr<PhysicalOperator> left, ClientContext &cxt, ChunkCollection *chunk_collection) {
	auto logical_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
	logical_join->types = {LogicalType::UBIGINT};
	vector<JoinCondition> conds; // TODO anything else for this one?
	unique_ptr<Expression> left_expression = make_unique<BoundReferenceExpression>("",LogicalType::UBIGINT,0);
	unique_ptr<Expression> right_expression = make_unique<BoundReferenceExpression>("",LogicalType::UBIGINT,0);
	JoinCondition cond = JoinCondition();
	cond.left = std::move(left_expression);
	cond.right = std::move(right_expression);
	cond.comparison = ExpressionType::COMPARE_EQUAL;
	cond.null_values_are_equal = false;
	conds.push_back(std::move(cond));
	vector<LogicalType> empty_types = {LogicalType::UBIGINT};
	const vector<column_t> cids = {0L};
	unique_ptr<ColumnBinding> col_bind = make_unique<ColumnBinding>(0,0);
	unique_ptr<BoundColumnRefExpression> exp = make_unique<BoundColumnRefExpression>(LogicalType::UBIGINT, *col_bind.get());
	vector<unique_ptr<Expression>> exps ;
	exps.push_back(std::move(exp));
	unique_ptr<Index> lineage_index = make_unique<LineageIndex>(cids, exps, op->lineage_op.at(-1));
	lineage_index->unbound_expressions.push_back(move(exp));
	lineage_index->column_ids.push_back(0);
	//lineage_index->op_lineage = op->lineage_op.at(-1);
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
	    1,true,
	    op->lineage_op.at(-1),
	    chunk_collection
	);
}


vector<unique_ptr<PhysicalOperator>> pipelines;

unique_ptr<PhysicalOperator> GenerateCustomPlan(PhysicalOperator* op, ClientContext &cxt, int lineage_id, unique_ptr<PhysicalOperator> left, bool simple_agg_flag) {
	if (!op) {
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
		return GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, move(left), true);
	}
	vector<LogicalType> types = {LogicalType::UBIGINT};
	PhysicalOperatorType op_type = PhysicalOperatorType::CHUNK_SCAN;
	idx_t estimated_cardinality = 1;
	if (left == nullptr) {
		unique_ptr<PhysicalChunkScan> chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
		DataChunk lineage_chunk;
		lineage_chunk.Initialize(types);
		lineage_chunk.SetCardinality(1);
		Vector lineage(Value::UBIGINT(lineage_id));
		lineage_chunk.data[0].Reference(lineage);
		chunk_scan->collection = new ChunkCollection();
		chunk_scan->collection->Append(lineage_chunk);
		if (op->children.size() == 2) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, build_chunk_scan->collection), false);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines.push_back(move(build_chunk_scan));
			} else {
				pipelines.push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false));
			}

			return custom_plan;
		}
		return GenerateCustomPlan(
		    op->children[0].get(),
		    cxt,
		    lineage_id,
		    PreparePhysicalIndexJoin(op, move(chunk_scan), cxt, nullptr),
		    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE
		);
	} else {
		if (op->children.size() == 2) {
			// Chunk scan that refers to build side of join
			unique_ptr<PhysicalChunkScan> build_chunk_scan = make_unique<PhysicalChunkScan>(types, op_type, estimated_cardinality, true);
			build_chunk_scan->collection = new ChunkCollection();

			// Probe side of join
			unique_ptr<PhysicalOperator> custom_plan = GenerateCustomPlan(op->children[0].get(), cxt, lineage_id, PreparePhysicalIndexJoin(op, move(left), cxt,  build_chunk_scan->collection), false);

			// Push build side chunk scan to pipelines
			if (op->type == PhysicalOperatorType::INDEX_JOIN) {
				pipelines.push_back(move(build_chunk_scan));
			} else {
				pipelines.push_back(GenerateCustomPlan(op->children[1].get(), cxt, lineage_id, move(build_chunk_scan), false));
			}

			// probe side of hash join
			return custom_plan;
		}
		return GenerateCustomPlan(
		    op->children[0].get(),
		    cxt,
		    lineage_id,
		    PreparePhysicalIndexJoin(op, move(left), cxt, nullptr),
		    op->type == PhysicalOperatorType::SIMPLE_AGGREGATE
		);
	}

}


string PragmaBackwardLineageDuckDBExecEngine(ClientContext &context, const FunctionParameters &parameters) {
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	string mode = parameters.values[1].ToString();
	int lineage_id = parameters.values[2].GetValue<int>();
	auto op = context.query_to_plan[query].get();
	unique_ptr<PhysicalOperator> plan = GenerateCustomPlan(op, context, lineage_id, nullptr, false);
	vector<unique_ptr<QueryResult>> results;
	results.push_back(context.RunPlan(plan.get()));
	for (idx_t i = 0; i < pipelines.size(); ++i) {
		// Iterate through pipelines backwards because we construct in reverse order
		results.push_back(context.RunPlan(pipelines[pipelines.size() - i - 1].get()));
	}
	pipelines.clear();
	if (op == nullptr) {
		throw std::logic_error("Querying non-existent lineage");
	}
	string str_results;
	for (idx_t i = 0; i < results.size(); i++) {
		unique_ptr<DataChunk> chunk = results[i]->Fetch();
		while (chunk != nullptr) {
			for (idx_t j = 0; j < chunk->size(); j++) {
				int val = chunk->GetValue(0, j).GetValue<int>();

				if (!str_results.empty()) {
					str_results += StringUtil::Format(", %d", val);
				} else {
					str_results += to_string(val);
				}
			}
			chunk = results[i]->Fetch();
		}
	}
	str_results = "list_value(" + str_results + ")";
	return StringUtil::Format("SELECT %s", str_results);
}

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
	set.AddFunction(PragmaFunction::PragmaCall("backward_lineage", PragmaBackwardLineage,  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	set.AddFunction(PragmaFunction::PragmaCall("lineage_query", PragmaBackwardLineageDuckDBExecEngine, {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER}));
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
