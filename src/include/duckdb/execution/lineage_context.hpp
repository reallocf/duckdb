//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_context.hpp
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


namespace duckdb {
class PhysicalOperator;
class ClientContext;
class LineageContext;


class ManageLineage {
public:
  ManageLineage(ClientContext &context) : context(context), query_id(0),
    queries_list_table_set(false)  {};

  void Reset();
  void AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext>  lineage);
  void AddLocalSinkLineage(PhysicalOperator*, vector< shared_ptr<LineageContext>>);

  void AnnotatePlan(PhysicalOperator *op);

  void CreateQueryTable();
  void logQuery(string input_query);
  void CreateLineageTables(PhysicalOperator *op);
  void Persist(PhysicalOperator* op, shared_ptr<LineageContext> lineage, bool is_sink);

  void BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx);

  idx_t op_id = 0;
  unordered_map<int, unordered_map<PhysicalOperator*, vector<shared_ptr<LineageContext>>>> pipelines_lineage;
  ClientContext &context;
  idx_t query_id;
  bool queries_list_table_set;
};


class LineageData {
public:
  LineageData() {}
  virtual unsigned long size_bytes() = 0;
  virtual void debug() = 0;
  virtual ~LineageData() {};
};

// A PassThrough to indicate that the operator doesn't affect lineage at all
// for example in the case of a Projection
class LineagePassThrough : public LineageData {
public:

  LineagePassThrough() {
#ifdef LINEAGE_DEBUG
    debug();
#endif
  }

  void debug() {
    std::cout << "LineagePassThrough" << std::endl;
  }

  unsigned long size_bytes() { return 0; }
};


template <class T>
class LineageDataVector : public LineageData {
public:
  LineageDataVector (vector<T> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
    debug();
#endif
  }

  void debug() {
    std::cout << "LineageDataVector " << " " << typeid(vec).name() << std::endl;
    for (idx_t i = 0; i < count; i++) {
      std::cout << " (" << i << " -> " << vec[i] << ") ";
    }
    std::cout << std::endl;
  }

  idx_t getAtIndex(idx_t idx) {
    return (idx_t)vec[idx];
  }

  unsigned long size_bytes() {
    return count * sizeof(vec[0]);
  }

  vector<T> vec;
  idx_t count;
};


template <typename T>
class LineageDataArray : public LineageData {
public:
  LineageDataArray (unique_ptr<T[]> vec_p, idx_t count, LogicalType type = LogicalType::INTEGER) :
    vec(move(vec_p)), count(count), type(type) {
#ifdef LINEAGE_DEBUG
    debug();
#endif
    }

  void debug() {
    std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
    for (idx_t i = 0; i < count; i++) {
      std::cout << " (" << i << " -> " << vec[i] << ") ";
    }
    std::cout << std::endl;
	}

  idx_t getAtIndex(idx_t idx) {
    return (idx_t)vec[idx];
  }

  int findIndexOf(idx_t data) {
    for (idx_t i = 0; i < count; i++) {
		  if (vec[i] == (T)data) return i;
    }
		return -1;
	}

  unsigned long size_bytes() {
		return count * sizeof(vec[0]);
	}

  unique_ptr<T[]> vec;
  idx_t count;
	LogicalType type;
};


class LineageSelVec : public LineageData {
public:
  LineageSelVec (SelectionVector vec_p, idx_t count, idx_t offset = 0, LogicalType type = LogicalType::INTEGER) :
    vec(move(vec_p)), count(count), type(type), offset(offset) {
#ifdef LINEAGE_DEBUG
    debug();
#endif
    }

  void debug() {
    std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
    for (idx_t i = 0; i < count; i++) {
      std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] << ") ";
    }
    std::cout << std::endl;
  }

  void getAllMatches(idx_t data, vector<idx_t> &matches) {
    for (idx_t i = 0; i < count; i++) {
      if (vec.get_index(i) == data) matches.push_back(i);
    }
  }

  idx_t getAtIndex(idx_t idx) {
    return vec.get_index(idx);
  }

  unsigned long size_bytes() {
    return count * sizeof(vec.get_index(0));
  }

  SelectionVector vec;
  idx_t count;
  LogicalType type;
	idx_t offset;
};

class LineageConstant : public LineageData {
public:

  LineageConstant(idx_t value) : value(value) {
#ifdef LINEAGE_DEBUG
    debug();
#endif
  }

  void debug() {
    std::cout << "LineageConstant: " << value << std::endl;
  }

  unsigned long size_bytes() {
    return sizeof(value);
  }

  idx_t value;
};


// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:

  LineageRange(idx_t start, idx_t end) : start(start), end(end) {
#ifdef LINEAGE_DEBUG
    debug();
#endif
  }

  void debug() {
    std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
  }

  unsigned long size_bytes() {
    return 2*sizeof(start);
  }

  idx_t start;
	idx_t end;
};


class LineageCollection: public LineageData {
public:
  void add(string key, unique_ptr<LineageData> data) {
    collection[key] = move(data);
	}

  void debug() {}
  unsigned long size_bytes() { return 0; }

  std::unordered_map<string, unique_ptr<LineageData>> collection;
};


// base operator for Unary and Binary
class LineageOp {
public:
  LineageOp()  {}

  virtual unsigned long size_bytes() = 0;
  virtual ~LineageOp() {};
};


class LineageOpCollection: public LineageOp {
public:
  LineageOpCollection(vector<shared_ptr<LineageOp>> op_p) : op(move(op_p)){}

  unsigned long size_bytes() { return 0; }

  vector<shared_ptr<LineageOp>> op;
};


class LineageOpUnary : public LineageOp {
public:
  LineageOpUnary(shared_ptr<LineageData> data_p) : data(move(data_p)){}

  unsigned long size_bytes() {
    if (data)  return data->size_bytes();
    return 0;
  }

  shared_ptr<LineageData> data;
};


class LineageOpBinary : public LineageOp {
public:
  LineageOpBinary()  : data_lhs(nullptr), data_rhs(nullptr) {}
  LineageOpBinary(shared_ptr<LineageData> data_lhs_p, shared_ptr<LineageData> data_rhs_p) : data_lhs(move(data_lhs_p)), data_rhs(move(data_rhs_p)) {}

  unsigned long size_bytes() {
	  unsigned long size = 0;
    if (data_lhs) size = data_lhs->size_bytes();
		if (data_rhs) size += data_rhs->size_bytes();
    return size;
  }

  void setLHS(shared_ptr<LineageData> lhs) {
    data_lhs = move(lhs);
  }

  void setRHS(shared_ptr<LineageData> rhs) {
    data_rhs = move(rhs);
  }

  shared_ptr<LineageData> data_lhs;
  shared_ptr<LineageData> data_rhs;
};


class LineageContext {
public:
  LineageContext() {
    chunk_id = 0;
  }

  void RegisterDataPerOp(idx_t key, shared_ptr<LineageOp> op, int type = 0) {
    ht[type][key] = move(op);
  }

	bool isEmpty() {
    return ht.empty();
	}

	shared_ptr<LineageOp> GetLineageOp(idx_t key, int type) {
    if (ht.find(type) == ht.end())
			return NULL;

		if (ht[type].find(key) == ht[type].end())
      return NULL;

    return ht[type][key];
	}

  std::unordered_map<int, std::unordered_map<idx_t, shared_ptr<LineageOp>>> ht;
	int32_t chunk_id;
};

} // namespace duckdb
#endif
