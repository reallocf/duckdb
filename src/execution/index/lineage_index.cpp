//
// Created by sughosh kaushik on 4/4/22.
//

#include "duckdb/execution/index/lineage_index/lineage_index.hpp"

#include <utility>

namespace duckdb{
static bool endsWith(const std::string& str, const std::string& suffix)
{
	return str.size() >= suffix.size() && 0 == str.compare(str.size()-suffix.size(), suffix.size(), suffix);
}

LineageIndex::LineageIndex(const vector<column_t> &column_ids, const vector<unique_ptr<Expression>> &unbound_expressions, shared_ptr<OperatorLineage> op_lineage, bool is_unique,
                             bool is_primary) : Index(IndexType::LINEAGE_INDEX, column_ids, unbound_expressions, is_unique, is_primary) {
	logical_types = {LogicalType::INTEGER};
	expression_result.Initialize(logical_types);
	is_little_endian = IsLittleEndian();
	/*if(table_name.find("hash_join") != -1 && endsWith(table_name,"_0")){
		this->cust_idx_type = LineageIndexType::HASH_JOIN_SINK;
	}
	else if(table_name.find("hash_join") != -1 && endsWith(table_name,"_1")){
		this->cust_idx_type = LineageIndexType::HASH_JOIN_PROBE;
	}
	else if(table_name.find("FILTER")!=-1){
		this->cust_idx_type = LineageIndexType::FILTER;
	}
	else if(table_name.find("LIMIT")!=-1){
		this->cust_idx_type = LineageIndexType::LIMIT;
	}
	else if(table_name.find("AGGREGATION")!=-1){
		this->cust_idx_type = LineageIndexType::AGGREGATION;
	}
	else if(table_name.find("GROUP_BY")!=-1){
		this->cust_idx_type = LineageIndexType::GROUP_BY;
	}*/
	for (idx_t i = 0; i < types.size(); i++) {
		switch (types[i]) {
		case PhysicalType::INT8:
		case PhysicalType::INT16:
		case PhysicalType::INT32:
		case PhysicalType::INT64:
		case PhysicalType::INT128:
		case PhysicalType::UINT8:
		case PhysicalType::UINT16:
		case PhysicalType::UINT32:
		case PhysicalType::UINT64:
			break;
		default:
			throw InvalidTypeException(logical_types[i], "Invalid type for index");
		}
	}
}

LineageIndex::~LineageIndex() noexcept {

}

bool LineageIndex::Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) {

}

void LineageIndex::Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) {

}

unique_ptr<IndexScanState> LineageIndex::InitializeScanSinglePredicate(Transaction &transaction, Value value, ExpressionType expressionType) {
	auto result = make_unique<LineageIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expressionType;
	return move(result);
}

unique_ptr<IndexScanState> LineageIndex::InitializeScanTwoPredicates(Transaction &transaction, Value low_value, ExpressionType low_expression_type, Value high_value, ExpressionType high_expression_type) {

}

bool LineageIndex::Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) {

}

bool LineageIndex::Scan(Transaction &transaction, DataTable &table, IndexScanState &state, idx_t max_count, vector<row_t> &result_ids) {
	return true;
}

bool LineageIndex::SearchEqual(LineageIndexScanState *state, idx_t max_count, vector<row_t> &result_ids) {
	return false;
}

void LineageIndex::SearchEqualJoinNoFetch(Value &equal_value, idx_t &result_size) {

}

void LineageIndex::VerifyAppend(DataChunk &chunk) {

}
}

