#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/lineage/lineage_query.hpp"

namespace duckdb {

#ifdef LINEAGE
string PragmaBackwardLineageDuckDBExecEngine(ClientContext &context, const FunctionParameters &parameters) {
	std::cout << "Starting lineage querying" << std::endl;
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	int lineage_id = parameters.values[1].GetValue<int>();
	string mode = parameters.values[2].ToString();
	bool should_count = parameters.values[3].GetValue<int>() != 0;
	auto op = context.query_to_plan[query].get();
	if (op == nullptr) {
		throw std::logic_error("Querying non-existent lineage");
	}
	LineageQuery lineage_query = LineageQuery();
	clock_t start = clock();
	std::cout << "Running lineage querying" << std::endl;
	unique_ptr<QueryResult> result = lineage_query.Run(op, context, mode, lineage_id, should_count);
	std::cout << "Finished lineage querying" << std::endl;
	clock_t execute = clock();

	string str_results;
	idx_t count = 0;
	std::cout << "Pulling chunk" << std::endl;
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
		std::cout << "Pulling next chunk" << std::endl;
		chunk = result->Fetch();
	}
	if (count > 1) {
		str_results = "list_value(" + str_results + ")";
	}
	clock_t end = clock();
	// Reset chunk
//	chunk->lineage_agg_data = make_unique<vector<shared_ptr<vector<SourceAndMaybeData>>>>();
//	chunk->inner_agg_idx = 0;
//	chunk->outer_agg_idx = 0;
//	chunk->next_lineage_agg_data = make_unique<vector<shared_ptr<vector<SourceAndMaybeData>>>>();
//	chunk->lineage_simple_agg_data = make_shared<vector<LineageDataWithOffset>>();
//	chunk->inner_simple_agg_idx = 0;
//	chunk->outer_simple_agg_idx = 0;
//	chunk->next_lineage_simple_agg_data = make_shared<vector<LineageDataWithOffset>>();
	std::cout << "Execute time: " << ((float) execute - start) / CLOCKS_PER_SEC << std::endl;
	std::cout << "List build time: " << ((float) end - execute) / CLOCKS_PER_SEC << std::endl;
	std::cout << "Total time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
	return StringUtil::Format("SELECT %s", str_results);
}

string PragmaClearLineage(ClientContext &context, const FunctionParameters &parameters) {
    context.query_to_plan.clear();
	return "SELECT 1";
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
	set.AddFunction(PragmaFunction::PragmaStatement("clear_lineage", PragmaClearLineage));
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
