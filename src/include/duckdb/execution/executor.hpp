//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <queue>

namespace duckdb {
class ClientContext;
class DataChunk;
class PhysicalOperator;
class PhysicalOperatorState;
class ThreadContext;
class Task;
class LineageContext;

struct ProducerToken;

class Executor {
	friend class Pipeline;
	friend class PipelineTask;

public:
	explicit Executor(ClientContext &context);
	~Executor();

	ClientContext &context;

    // lineage for tree starting from this operator
	// 0: get chunk
	// 1: sink
    unordered_map<int, unordered_map<PhysicalOperator*, vector<shared_ptr<LineageContext>>>> pipelines_lineage;

public:
	void Initialize(PhysicalOperator *physical_plan);
	void BuildPipelines(PhysicalOperator *op, Pipeline *parent);

	vector<LogicalType> GetTypes();

	unique_ptr<DataChunk> FetchChunk();

	//! Push a new error
	void PushError(const string &exception);

	//! Flush a thread context into the client context
	void Flush(ThreadContext &context);

    void AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext>  lineage);
    void AddLocalSinkLineage(PhysicalOperator* opKey,  shared_ptr<LineageContext> lineage);
    void BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx);
    void ForwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int idx);
    void Reset();
    void LineageSize();
    void QueryLineage(shared_ptr<LineageContext> lineage);

	//! Returns the progress of the pipelines
	bool GetPipelinesProgress(int &current_progress);

private:
	PhysicalOperator *physical_plan;
	unique_ptr<PhysicalOperatorState> physical_state;

	mutex executor_lock;
	//! The pipelines of the current query
	vector<unique_ptr<Pipeline>> pipelines;
	//! The producer of this query
	unique_ptr<ProducerToken> producer;
	//! Exceptions that occurred during the execution of the current query
	vector<string> exceptions;

	//! The amount of completed pipelines of the query
	std::atomic<idx_t> completed_pipelines;
	//! The total amount of pipelines in the query
	idx_t total_pipelines;

	unordered_map<PhysicalOperator *, Pipeline *> delim_join_dependencies;
	PhysicalOperator *recursive_cte;
};
} // namespace duckdb
