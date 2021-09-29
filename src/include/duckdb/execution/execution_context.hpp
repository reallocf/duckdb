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

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, TaskContext &task_p)
	    : client(client_p), thread(thread_p), task(task_p) {
#ifdef LINEAGE
    lineage = make_shared<LineageContext>();
#endif
	}

#ifdef LINEAGE
  void setCurrent(idx_t op_id) {
    current_op_id = op_id;
  }

  idx_t getCurrent() {
    return current_op_id;
  }
#endif

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! The task context for this execution
	TaskContext &task;
#ifdef LINEAGE
	//! The lineage context for this execution
  shared_ptr<LineageContext> lineage;
	//! Current operator of this execution
  idx_t current_op_id;
#endif
};

} // namespace duckdb
