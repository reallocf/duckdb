//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include <iostream>
#include <utility>

#ifndef LINEAGE_UNARY

// Define meaningful lineage_idx names
#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
class PhysicalOperator;
class ClientContext;
class LineageData;


class LineageManager {
public:
	explicit LineageManager(ClientContext &context) : context(context) {};

	void AnnotatePlan(PhysicalOperator *op);
	void CreateLineageTables(PhysicalOperator *op);
	void CreateQueryTable();
	void LogQuery(const string& input_query);

	ClientContext &context;
	idx_t query_id = 0;
	string query_list_table_name = "queries_list";
};


struct LineageProcessStruct {
	idx_t count_so_far;
	bool still_processing;
};


class LineageOp {
public:
	LineageOp()  {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx);
	void FinishedProcessing();
	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk);
	idx_t Size();

	// data[0] used by all ops; data[1] used by pipeline breakers
	// TODO does this need to have a shared_ptr wrapper?
	std::vector<shared_ptr<LineageData>> data[2];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
};


class LineageData {
public:
	virtual idx_t Count() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(idx_t count_so_far) = 0;
	virtual idx_t Size() = 0;
	virtual ~LineageData() {};
};


// TODO get templating working like before - that would be better

class LineageDataRowVector : public LineageData {
public:
	LineageDataRowVector(vector<row_t> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	vector<row_t> vec;
	idx_t count;
};


class LineageDataUIntPtrArray : public LineageData {
public:
	LineageDataUIntPtrArray(unique_ptr<uintptr_t[]> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	unique_ptr<uintptr_t[]> vec;
	idx_t count;
};


class LineageDataUInt32Array : public LineageData {
public:
	LineageDataUInt32Array(unique_ptr<uint32_t[]>vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	unique_ptr<uint32_t[]> vec;
	idx_t count;
};


class LineageSelVec : public LineageData {
public:
	LineageSelVec(const SelectionVector& vec_p, idx_t count) : vec(vec_p), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	SelectionVector vec;
	idx_t count;
};


// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:
	LineageRange(idx_t start, idx_t end) : start(start), end(end) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	idx_t start;
	idx_t end;
	vector<sel_t> vec;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinaryData : public LineageData {
public:
	LineageBinaryData(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      left(std::move(lhs)), right(std::move(rhs)) {
		D_ASSERT(left->Count() == right->Count());
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t count_so_far) override;
	idx_t Size() override;

	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
	bool switch_on_left = true;
};


} // namespace duckdb
#endif
