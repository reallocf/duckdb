//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include <iostream>

namespace duckdb {
class PhysicalOperator;

class LineageData {
public:
    LineageData() {}

    virtual unsigned long size_bytes() = 0;
    virtual idx_t getAtIndex(idx_t idx) = 0;
	virtual void debug() = 0;
	virtual idx_t findIndexOf(idx_t data) = 0;
	virtual void getAllMatches(idx_t data, vector<idx_t> &matches)  = 0;
};

template <typename T>
class LineageDataVector : public LineageData {
public:

    LineageDataVector (vector<T> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
        this->debug();
#endif
    }

    void debug() {
        std::cout << "LineageDataVector " << " " << typeid(vec).name() << std::endl;
        for (idx_t i = 0; i < count; i++) {
            std::cout << " (" << i << " -> " << vec[i] << ") ";
        }
        std::cout << std::endl;
    }

    idx_t findIndexOf(idx_t data) {
        for (idx_t i = 0; i < count; i++) {
            if (vec[i] == (T)data) return i;
        }
        return 0;
    }
    void getAllMatches(idx_t data, vector<idx_t> &matches) {
		 for (idx_t i = 0; i < count; i++) {
            if (vec[i] == (T)data) matches.push_back(i);
        }
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

    LineageDataArray (unique_ptr<T[]> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
    this->debug();
#endif
    }

	void debug() {
        std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
        for (idx_t i = 0; i < count; i++) {
            std::cout << " (" << i << " -> " << vec[i] << ") ";
        }
        std::cout << std::endl;
	}

    void getAllMatches(idx_t data, vector<idx_t> &matches) {
		 for (idx_t i = 0; i < count; i++) {
            if (vec[i] == (T)data) matches.push_back(i);
        }
	}

    idx_t findIndexOf(idx_t data) {
        for (idx_t i = 0; i < count; i++) {
			if (vec[i] == (T)data) return i;
        }
		return 0;
	}

	idx_t getAtIndex(idx_t idx) {
        return (idx_t)vec[idx];
	}

	unsigned long size_bytes() {
		return count * sizeof(vec[0]);
	}

    unique_ptr<T[]> vec;
    idx_t count;
};

// A PassThrough to indicate that the operator doesn't affect lineage at all
// for example in the case of a Projection
class LineagePassThrough : public LineageData {
public:

	LineagePassThrough() {}

	void debug() {}
    idx_t findIndexOf(idx_t data) {return 5;}

    unsigned long size_bytes() {
        return 0;
    }

    void getAllMatches(idx_t data, vector<idx_t> &matches) {}

    idx_t getAtIndex(idx_t idx) { return 0; }
};

// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:

	LineageRange(idx_t start, idx_t end) : start(start), end(end) {
#ifdef LINEAGE_DEBUG
		this.debug();
#endif
	}

    void debug() {
        std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
    }
    idx_t findIndexOf(idx_t data) {return 5;}

    unsigned long size_bytes() {
        return 2*sizeof(start);
    }
    void getAllMatches(idx_t data, vector<idx_t> &matches) {}

    idx_t getAtIndex(idx_t idx) { return 0; }

    idx_t start;
	idx_t end;
};

// A Reduce indicates that all input values lead to a single output
// such as for simple aggregations COUNT(*) FROM foo
class LineageReduce : public LineageData {
public:
	LineageReduce() {
	}
    void debug() {}
    unsigned long size_bytes() {
        return 0;
    }
    idx_t findIndexOf(idx_t data) {return 5;}
    void getAllMatches(idx_t data, vector<idx_t> &matches) {}

    idx_t getAtIndex(idx_t idx) { return 0; }
};

class LineageCollection: public LineageData {
public:
    void add(string key, unique_ptr<LineageData> data) {
		collection[key] = move(data);
	}

    void debug() {}
    unsigned long size_bytes() {
        return 0;
    }
    idx_t findIndexOf(idx_t data) {return 5;}
    void getAllMatches(idx_t data, vector<idx_t> &matches) {}
    idx_t getAtIndex(idx_t idx) { return 0; }

    std::unordered_map<string, unique_ptr<LineageData>> collection;
};


// base operator for Unary and Binary
class LineageOp {
public:
    LineageOp()  {}

    virtual unsigned long size_bytes() = 0;
};

class LineageOpCollection: public LineageOp {
public:
    LineageOpCollection(vector<shared_ptr<LineageOp>> op_p) : op(move(op_p)){}

    unsigned long size_bytes() {
        return 0;
    }

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

    }
    void RegisterDataPerOp(PhysicalOperator* key, shared_ptr<LineageOp> op, int type = 0) {
        ht[type][key] = move(op);
    }

	bool isEmpty() {
		return ht.empty();
	}

	shared_ptr<LineageOp> GetLineageOp(PhysicalOperator* key, int type) {
		if (ht.find(type) == ht.end()) {
			std::cout << "GetLineageOp type not found for " << type << std::endl;
			return NULL;
		}
		if (ht[type].find(key) == ht[type].end()) {
            std::cout << "GetLineageOp key not found " << std::endl;
            return NULL;
		}
		return ht[type][key];
	}
	/*unsigned long size_bytes() {
		unsigned long size = 0;
        for (const auto& elm : ht) {
			if (elm.second)
			    size += elm.second->size_bytes();
		}
		return size;
	}

    unsigned long  size_per_op(std::unordered_map<PhysicalOperator*, unsigned long> &ht_size) {
        unsigned long size = 0;
        for (const auto& elm : ht) {
			if (!elm.second || !elm.first) continue;
            size += elm.second->size_bytes();
			if (ht_size.find(elm.first) == ht_size.end())
                ht_size[elm.first] = elm.second->size_bytes();
			else
                ht_size[elm.first] += elm.second->size_bytes();
        }

		return size;
    }*/

    std::unordered_map<int, std::unordered_map<PhysicalOperator*, shared_ptr<LineageOp>>> ht;
};

} // namespace duckdb
