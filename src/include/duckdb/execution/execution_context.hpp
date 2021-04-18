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
class ClientContext;
class ThreadContext;
class TaskContext;

class LineageData {
public:
    LineageData() {}
};

class LineageDataSelVec : public LineageData {
public:
    LineageDataSelVec(SelectionVector sel_p, idx_t count) : sel(sel_p), count(count) {
        std::cout << "LineageDataSelVecL " << sel.ToString(count) << std::endl;
    }
    SelectionVector sel;
    idx_t count;
};

class LineageDataVector : public LineageData {
public:
    LineageDataVector(Vector vec_p, idx_t count) : vec(move(vec_p)), count(count) {
        std::cout << "LineageDataVector " << vec.ToString(count) << std::endl;
    }

    Vector vec;
    idx_t count;
};

// base operator for Unary and Binary
class LineageOp {
public:
    LineageOp()  {}
};

class LineageOpUnary : public LineageOp {
public:
    LineageOpUnary(unique_ptr<LineageData> data_p) : data(move(data_p)){}

    unique_ptr<LineageData> data;
};

class LineageOpBinary : public LineageOp {
public:
    LineageOpBinary()  : data_lhs(nullptr), data_rhs(nullptr) {}
    LineageOpBinary(unique_ptr<LineageData> data_lhs_p, unique_ptr<LineageData> data_rhs_p) : data_lhs(move(data_lhs_p)), data_rhs(move(data_rhs_p)) {}

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

    std::unordered_map<void*, unique_ptr<LineageOp>> ht;
};

class ExecutionContext {
public:

    ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, TaskContext &task_p)
        : client(client_p), thread(thread_p), task(task_p) {
        lineage = make_unique<LineageContext>();
    }

    //! The client-global context; caution needs to be taken when used in parallel situations
    ClientContext &client;
    //! The thread-local context for this execution
    ThreadContext &thread;
    //! The task context for this execution
    TaskContext &task;
    //! The lineage context for this execution
    unique_ptr<LineageContext> lineage;
};


} // namespace duckdb
