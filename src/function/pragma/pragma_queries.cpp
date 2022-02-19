#include "duckdb/function/pragma/pragma_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

#ifdef LINEAGE
string PragmaBackwardLineage(ClientContext &context, const FunctionParameters &parameters) {
	// query the lineage data, create a view on top of it, and then query that
	string query = parameters.values[0].ToString();
	auto op = context.query_to_plan[query].get();
	if (op == nullptr) {
		throw std::logic_error("Querying non-existent lineage");
	}

	string origin = parameters.values[1].ToString();
	std::stringstream ss(origin);
	string word;
	idx_t out;
	while (ss >> word) {
		clock_t start = clock();
		auto lineage = context.lineage_manager->Backward(op, (idx_t)stoi(word));
		clock_t end = clock();
		std::cout << "Root Backward time: " << ((float) end - start) / CLOCKS_PER_SEC << std::endl;
		out = lineage.size();
	}

	return StringUtil::Format("SELECT %i", out);
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
	set.AddFunction(PragmaFunction::PragmaCall("backward_lineage", PragmaBackwardLineage,  {LogicalType::VARCHAR, LogicalType::VARCHAR}));
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
