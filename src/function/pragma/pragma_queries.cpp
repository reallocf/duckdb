#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/index/lineage_index/lineage_index.hpp"

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

//static std::queue<std::unique_ptr<PhysicalOperator>> idx_join_ops;
//static std::queue<std::unique_ptr<PhysicalOperator>> lineage_scan_ops;

std::unique_ptr<PhysicalIndexJoin> preparePhysicalIndexJoin(PhysicalOperator *op, ClientContext &cxt){
	vector<LogicalType> types;
	types.emplace_back(LogicalType::INTEGER);
	types.emplace_back(LogicalType::INTEGER);
	auto logical_join = make_unique<LogicalComparisonJoin>(JoinType::INNER);
	logical_join->types = types;
	shared_ptr<OperatorLineage> opLineage = op->lineage_op.at(-1);
	vector<column_t> cids;
	cids.push_back(1L);
	vector<unique_ptr<Expression>> unbound_expressions;
	unique_ptr<Index> index = make_unique<Lineage_Index>(cids, unbound_expressions, "", true, false);
	auto join =  make_unique<PhysicalIndexJoin>(*logical_join.get(), index.get());
	return join;
}


void GenerateCustomPlan(PhysicalOperator* op, unique_ptr<PhysicalOperator> physicalOperator,ClientContext &cxt){
	if(op->children[0]){
		GenerateCustomPlan(op->children[0].get(), NULL, cxt);
	}
	unique_ptr<PhysicalOperator> idx_join = preparePhysicalIndexJoin(op, cxt);
	idx_join->children.push_back(move(physicalOperator));
	if(physicalOperator == NULL){
		//prepare a scan operator

	}
	physicalOperator = move(idx_join);
	return;
}

string PragmaBackwardLineageDuckDBExecEngine(ClientContext &context, const FunctionParameters &parameters) {
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	string mode = parameters.values[1].ToString();
	auto op = context.query_to_plan[query].get();
	GenerateCustomPlan(op, NULL, context);
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
			for (const SourceAndMaybeData& el : lineage) {
				if (!out_str.empty()) {
					out_str += ",";
				}
				out_str += to_string(el.source);
			}
		}
		out_str = "list_value("+ out_str +")";

		return StringUtil::Format("SELECT %s", out_str);
	} else if (mode == "COUNT") {
		idx_t out_idx;
		while (ss >> word) {
			clock_t start = clock();
			out_idx = context.lineage_manager->BackwardCount(op, (idx_t)stoi(word));
			clock_t end = clock();
			std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		}

		return StringUtil::Format("SELECT %i", out_idx);
	} else {
		throw std::logic_error("Invalid backward query mode - should be VALUE or COUNT");
	}
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
