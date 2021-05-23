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

class LineageData {
public:
    LineageData() {}

    virtual unsigned long size_bytes() = 0;
};

class LineageDataVector : public LineageData {
public:
    LineageDataVector(Vector vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
        std::cout << "LineageDataVector " << vec.ToString(count) << std::endl;
#endif
    }

    unsigned long size_bytes() {
        return count * sizeof(vec.GetValue(0));
    }

    Vector vec;
    idx_t count;
};

template <class T>
class LineageDataArray : public LineageData {
public:

    LineageDataArray (T *vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
        std::cout << "LineageDataArray " << " " << typeid(vec_p).name() << std::endl;
        for (idx_t i = 0; i < count; i++) {
            std::cout << " (" << i << " -> " << vec_p[i] << ") ";
        }
        std::cout << std::endl;
#endif
    }

	unsigned long size_bytes() {
		return count * sizeof(vec[0]);
	}

    T *vec;
    idx_t count;
};


class LineageCollection : public LineageData {
public:

	LineageCollection() : size(0) {}

	void add(unique_ptr<LineageData> new_data) {
		size += new_data->size_bytes();
        data.push_back(move(new_data));
	}

    unsigned long size_bytes() {
        return size;
    }

    vector<unique_ptr<LineageData>> data;
	unsigned long size;
};

// A PassThrough to indicate that the operator doesn't affect lineage at all
// for example in the case of a Projection
class LineagePassThrough : public LineageData {
public:

	LineagePassThrough() {}

    unsigned long size_bytes() {
        return 0;
    }
};

// A Range of values where each successive number in the range indicates the lineage
// used to quickly capture Limits
class LineageRange : public LineageData {
public:

	LineageRange(idx_t start, idx_t end) : start(start), end(end) {
#ifdef LINEAGE_DEBUG
        std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
#endif
	}

    unsigned long size_bytes() {
        return 2*sizeof(start);
    }

	idx_t start;
	idx_t end;
};

// A Reduce indicates that all input values lead to a single output
// such as for simple aggregations COUNT(*) FROM foo
class LineageReduce : public LineageData {
public:

    LineageReduce() {}

    unsigned long size_bytes() {
        return 0;
    }
};

// base operator for Unary and Binary
class LineageOp {
public:
    LineageOp()  {}

    virtual unsigned long size_bytes() = 0;
};

class LineageOpUnary : public LineageOp {
public:
    LineageOpUnary(unique_ptr<LineageData> data_p) : data(move(data_p)){}

    unsigned long size_bytes() {
		if (data)  return data->size_bytes();
        return 0;
    }

    unique_ptr<LineageData> data;
};

class LineageOpBinary : public LineageOp {
public:
    LineageOpBinary()  : data_lhs(nullptr), data_rhs(nullptr) {}
    LineageOpBinary(unique_ptr<LineageData> data_lhs_p, unique_ptr<LineageData> data_rhs_p) : data_lhs(move(data_lhs_p)), data_rhs(move(data_rhs_p)) {}

    unsigned long size_bytes() {
		unsigned long size = 0;
        if (data_lhs) size = data_lhs->size_bytes();
		if (data_rhs) size += data_rhs->size_bytes();
        return size;
    }

    void setLHS(unique_ptr<LineageData> lhs) {
        data_lhs = move(lhs);
    }

    void setRHS(unique_ptr<LineageData> rhs) {
        data_rhs = move(rhs);
    }

    unique_ptr<LineageData> data_lhs;
    unique_ptr<LineageData> data_rhs;
};


class LineageContext {
public:
    LineageContext() {

    }
    void RegisterDataPerOp(void* key, unique_ptr<LineageOp> op) {
        ht[key] = move(op);
    }

	bool isEmpty() {
		return ht.empty();
	}

	unsigned long size_bytes() {
		unsigned long size = 0;
        for (const auto& elm : ht) {
			if (elm.second)
			    size += elm.second->size_bytes();
		}
		return size;
	}

    std::unordered_map<void*, unique_ptr<LineageOp>> ht;
};

} // namespace duckdb
