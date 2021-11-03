//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_data.hpp
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
#include "duckdb/execution/lineage/pipeline_lineage.hpp"

#include <iostream>
#include <utility>

namespace duckdb {

class LineageData {
public:
	virtual idx_t Count() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(idx_t offset) = 0;
	virtual idx_t Size() = 0;
	virtual ~LineageData() {};
};

struct LineageDataWithOffset {
	// TODO does this need to have a shared_ptr wrapper?
	shared_ptr<LineageData> data;
	idx_t offset;
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
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

private:
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
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

private:
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
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

private:
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
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

	// TODO should this be a func shared across all LineageData?
	vector<LineageDataWithOffset> Divide();

private:
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
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

private:
	idx_t start;
	idx_t end;
	vector<sel_t> vec;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinary : public LineageData {
public:
	LineageBinary(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      left(move(lhs)), right(move(rhs)) {
		D_ASSERT(left->Count() == right->Count());
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	idx_t Size() override;

private:
	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
	bool switch_on_left = true;
};


} // namespace duckdb
#endif
