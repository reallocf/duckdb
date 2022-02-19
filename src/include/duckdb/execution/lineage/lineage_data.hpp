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
struct LineageDataWithOffset;

class LineageData {
public:
	virtual idx_t Count() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(idx_t offset) = 0;
	virtual void SetChild(shared_ptr<LineageDataWithOffset> c) = 0;
	virtual shared_ptr<LineageDataWithOffset> GetChild() = 0;
	virtual idx_t Size() = 0;
	virtual idx_t Backward(idx_t) = 0;
	virtual ~LineageData() {};
};

struct LineageDataWithOffset {
	// TODO does this need to have a shared_ptr wrapper?
	shared_ptr<LineageData> data;
	int child_offset;
	idx_t this_offset;
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
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	vector<row_t> vec;
	idx_t count;
	shared_ptr<LineageDataWithOffset> child;
};

class LineageDataVectorBufferArray : public LineageData {
public:
	LineageDataVectorBufferArray(unique_ptr<data_t[]> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	unique_ptr<data_t[]> vec;
	idx_t count;
	shared_ptr<LineageDataWithOffset> child;
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
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	unique_ptr<uintptr_t[]> vec;
	idx_t count;
	shared_ptr<LineageDataWithOffset> child;
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
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	unique_ptr<uint32_t[]> vec;
	idx_t count;
	shared_ptr<LineageDataWithOffset> child;
};

class LineageSelVec : public LineageData {
public:
	LineageSelVec(const SelectionVector& vec_p, idx_t count, idx_t in_offset=0) : vec(vec_p), count(count), in_offset(in_offset) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

	// TODO should this be a func shared across all LineageData?
	vector<LineageDataWithOffset> Divide();

private:
	SelectionVector vec;
	idx_t count;
	shared_ptr<LineageDataWithOffset> child;
	idx_t in_offset;
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
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	idx_t start;
	idx_t end;
	vector<sel_t> vec;
	shared_ptr<LineageDataWithOffset> child;
};

// Constant Value
class LineageConstant : public LineageData {
public:
	LineageConstant(idx_t value, idx_t count) : value(value), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

private:
	idx_t value;
	idx_t count;
	vector<int> vec;
	shared_ptr<LineageDataWithOffset> child;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinary : public LineageData {
public:
	LineageBinary(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      left(move(lhs)), right(move(rhs)) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
private:
	bool switch_on_left = true;
	shared_ptr<LineageDataWithOffset> child;
};

class LineageNested : public LineageData {
public:
	LineageNested() {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	explicit LineageNested(const shared_ptr<LineageDataWithOffset>& lineage_data) : lineage({lineage_data}) {
		count = lineage_data->data->Count();
		size = lineage_data->data->Size();
		index.push_back(count);
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	idx_t Count() override;
	void Debug() override;
	data_ptr_t Process(idx_t offset) override;
	void SetChild(shared_ptr<LineageDataWithOffset> c) override;
	shared_ptr<LineageDataWithOffset> GetChild() override;
	idx_t Size() override;
	idx_t Backward(idx_t) override;

	void AddLineage(const shared_ptr<LineageDataWithOffset>& lineage_data);
	shared_ptr<LineageDataWithOffset> GetInternal();
	bool IsComplete();
	shared_ptr<LineageDataWithOffset>& GetChunkAt(idx_t index);
	int LocateChunkIndex(idx_t source);
	idx_t GetAccCount(idx_t i);

private:
	vector<shared_ptr<LineageDataWithOffset>> lineage = {};
	idx_t ret_idx = 0;
	idx_t count = 0;
	idx_t size = 0;
	vector<idx_t> index;
};


} // namespace duckdb
#endif
