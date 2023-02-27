#include "duckdb/main/stream_query_result.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

StreamQueryResult::StreamQueryResult(StatementType statement_type, shared_ptr<ClientContext> context,
                                     vector<LogicalType> types, vector<string> names,
                                     shared_ptr<PreparedStatementData> prepared)
    : QueryResult(QueryResultType::STREAM_RESULT, statement_type, move(types), move(names)), is_open(true),
      context(move(context)), prepared(move(prepared)) {
}

StreamQueryResult::~StreamQueryResult() {
	Close();
}

string StreamQueryResult::ToString() {
	string result;
	if (success) {
		result = HeaderToString();
		result += "[[STREAM RESULT]]";
	} else {
		result = error + "\n";
	}
	return result;
}

unique_ptr<DataChunk> StreamQueryResult::FetchRaw() {
	if (!success || !is_open) {
		throw InvalidInputException(
		    "Attempting to fetch from an unsuccessful or closed streaming query result\nError: %s", error);
	}
	auto chunk = context->Fetch();
	if (!chunk || chunk->ColumnCount() == 0 || chunk->size() == 0) {
		Close();
		return nullptr;
	}
	return chunk;
}

unique_ptr<MaterializedQueryResult> StreamQueryResult::Materialize() {
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	auto result = make_unique<MaterializedQueryResult>(statement_type, types, names);
	while (true) {
		auto chunk = Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		result->collection.Append(*chunk);
	}
	if (!success) {
		return make_unique<MaterializedQueryResult>(error);
	}
	return result;
}

void StreamQueryResult::Close() {
	if (!is_open) {
		return;
	}
	is_open = false;
#ifdef LINEAGE
	std::cout << "Foo1" << std::endl;
	if (context->trace_lineage) {
//		idx_t lineage_size = context->lineage_manager->CreateLineageTables(prepared->plan.get());
		std::cout << "Foo2" << std::endl;
		if (LINEAGE_INDEXES_ON) {
			clock_t start = clock();
			std::cout << "Foo3" << std::endl;
			context->lineage_manager->PostProcess(prepared->plan.get());
			std::cout << "Foo4" << std::endl;
			clock_t end = clock();
			std::cout << "PostProcess time: " << ((float) end - start) / CLOCKS_PER_SEC << " sec" << std::endl;
		}
		std::cout << "Foo5" << std::endl;
		context->lineage_manager->LogQuery(context->query, 0);
		std::cout << "Foo6" << std::endl;
		context->query_to_plan[context->query] = move(prepared->plan);
		std::cout << "Foo7" << std::endl;
	}
#endif
	context->Cleanup();
}

} // namespace duckdb
