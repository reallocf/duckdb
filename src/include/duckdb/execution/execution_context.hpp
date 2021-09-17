//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/lineage_context.hpp"

namespace duckdb {
class ClientContext;
class ThreadContext;
class TaskContext;
class LineageContext;
class PhysicalOperator;

class ExecutionContext {
public:

    ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, TaskContext &task_p, bool trace_lineage)
        : client(client_p), thread(thread_p), task(task_p), lineage(make_shared<LineageContext>()),
	      trace_lineage(trace_lineage) {
    }

    void setCurrent(PhysicalOperator * op) {
      current_op = op;
    }

    PhysicalOperator *getCurrent() {
		return current_op;
    };

    //! The client-global context; caution needs to be taken when used in parallel situations
    ClientContext &client;
    //! The thread-local context for this execution
    ThreadContext &thread;
    //! The task context for this execution
    TaskContext &task;
    //! The lineage context for this execution
    shared_ptr<LineageContext> lineage;
    PhysicalOperator* current_op;
	bool trace_lineage;
};

} // namespace duckdb
