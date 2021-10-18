//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/common/common.hpp"
#ifdef LINEAGE
#include "duckdb/execution/lineage.hpp"
#endif

namespace duckdb {
class ClientContext;
class ThreadContext;
class TaskContext;

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, TaskContext &task_p)
	    : client(client_p), thread(thread_p), task(task_p) {
	}

#ifdef LINEAGE
  void SetCurrentLineageOp(shared_ptr<LineageOp> lop) {
    current_lop = move(lop);
  }

  shared_ptr<LineageOp> GetCurrentLineageOp() {
    return current_lop;
  }
#endif

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! The task context for this execution
	TaskContext &task;
#ifdef LINEAGE
	//! Current operator of this execution
	shared_ptr<LineageOp> current_lop;
#endif
};

} // namespace duckdb
