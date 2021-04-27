#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/storage/storage_manager.hpp"

namespace duckdb {

struct PragmaDatabaseListData : public FunctionOperatorData {
	PragmaDatabaseListData() : finished(false) {
	}

	bool finished;
};

static unique_ptr<FunctionData> PragmaDatabaseListBind(ClientContext &context, vector<Value> &inputs,
                                                       unordered_map<string, Value> &named_parameters,
                                                       vector<LogicalType> &input_table_types,
                                                       vector<string> &input_table_names,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("seq");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("name");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("file");
	return_types.push_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<FunctionOperatorData> PragmaDatabaseListInit(ClientContext &context, const FunctionData *bind_data,
                                                        const vector<column_t> &column_ids,
                                                        TableFilterCollection *filters) {
	return make_unique<PragmaDatabaseListData>();
}

#ifdef LINEAGE
void PragmaDatabaseListFunction(ExecutionContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
#else
void PragmaDatabaseListFunction(ClientContext &context, const FunctionData *bind_data,
                                FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
#endif
	auto &data = (PragmaDatabaseListData &)*operator_state;
	if (data.finished) {
		return;
	}

	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::INTEGER(0));
	output.data[1].SetValue(0, Value("main"));
#ifdef LINEAGE
	output.data[2].SetValue(0, Value(StorageManager::GetStorageManager(context.client).GetDBPath()));
#else
	output.data[2].SetValue(0, Value(StorageManager::GetStorageManager(context).GetDBPath()));
#endif

	data.finished = true;
}

void PragmaDatabaseList::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_database_list", {}, PragmaDatabaseListFunction, PragmaDatabaseListBind,
	                              PragmaDatabaseListInit));
}

} // namespace duckdb
