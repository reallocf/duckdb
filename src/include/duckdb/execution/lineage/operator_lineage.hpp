//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator_lineage.hpp
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
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/execution/lineage/pipeline_lineage.hpp"

#include <experimental/coroutine>
#include <iostream>
#include <utility>

#ifndef LINEAGE_UNARY

// Define meaningful lineage_idx names
#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_COMBINE 2
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

namespace duckdb {
template<typename T> class Generator;
class LineageRes;
enum class PhysicalOperatorType : uint8_t;
struct LineageDataWithOffset;
struct LineageProcessStruct;
struct SimpleAggQueryStruct;
struct SourceAndMaybeData;
struct LineageIndexStruct;

enum LineageJoinType {
	LIN,
	PERM,
	PROV
};

class OperatorLineage {
public:
	explicit OperatorLineage(
		shared_ptr<PipelineLineage> pipeline_lineage,
		std::vector<shared_ptr<OperatorLineage>> children,
	    PhysicalOperatorType type,
	    idx_t opid,
	    bool should_index
	) : opid(opid), pipeline_lineage(move(pipeline_lineage)), type(type), children(move(children)), should_index(should_index) {}

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx, int thread_id=-1);

	void FetchResultChunk(Value equal_value, DataChunk& result_chunk);

	void FinishedProcessing(idx_t data_idx, idx_t finished_idx);
	shared_ptr<PipelineLineage> GetPipelineLineage();
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void MarkChunkReturned();
	LineageProcessStruct Process(const vector<column_t> column_ids, const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk, idx_t size=0, int thread_id=-1, idx_t data_idx = 0, idx_t finished_idx = 0);

	LineageProcessStruct PostProcess(idx_t chunk_count, idx_t count_so_far, idx_t data_idx = 0, idx_t finished_idx = 0);
	Generator<shared_ptr<vector<SourceAndMaybeData>>> Backward(shared_ptr<vector<SourceAndMaybeData>> lineage, LineageJoinType join_type);
	// Leaky... should refactor this so we don't need a pure pass-through function like this
	void SetChunkId(idx_t idx);
	idx_t Size();
	shared_ptr<LineageDataWithOffset> GetMyLatest();
	shared_ptr<LineageDataWithOffset> GetChildLatest(idx_t lineage_idx);
	idx_t GetThisOffset(idx_t lineage_idx);
	SimpleAggQueryStruct RecurseForSimpleAgg(const shared_ptr<OperatorLineage>& child);

	void AccessIndex(LineageIndexStruct val);

public:
	idx_t opid;
	bool trace_lineage;
	shared_ptr<PipelineLineage> pipeline_lineage;
	// data[0] used by all ops; data[1] used by pipeline breakers
	// Lineage data in here!
	std::vector<LineageDataWithOffset> data[3];
	PhysicalOperatorType type;
	shared_ptr<LineageNested> cached_internal_lineage = nullptr;
	std::vector<shared_ptr<OperatorLineage>> children;
	// final lineage indexing data-structures
	// hash_chunk_count: maintain count of data that belong to previous ranges
	vector<idx_t> hash_chunk_count;
	// hm_range: maintains the existing ranges in hash join build side
	std::vector<std::pair<idx_t, idx_t>> hm_range;
	// offset: difference between two consecutive values with a range
	uint64_t offset = 0;
	idx_t start_base = 0;
	idx_t last_base = 0;

	// Index for hash aggregate
    std::unordered_map<idx_t, vector<SourceAndMaybeData>> hash_map_agg;
    // index: used to index selection vectors
    //        it stores the size of SV from each chunk
    //        which helps in locating the one needed
    //        using binary-search.
    // Index for when we need to identify the chunk from a global offset
    vector<idx_t> index;
    bool should_index;
};

struct LineageProcessStruct {
	LineageProcessStruct(idx_t i, idx_t i1, idx_t i2, idx_t i3, bool b);
	idx_t count_so_far;
	idx_t size_so_far;
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
	bool still_processing;
};

struct SimpleAggQueryStruct {
	shared_ptr<OperatorLineage> materialized_child_op;
	vector<LineageDataWithOffset> child_lineage_data_vector;
};

struct SourceAndMaybeData {
	idx_t source;
	shared_ptr<LineageDataWithOffset> data;
};

struct LineageIndexStruct {
	// Input chunk that we transform via the index to replace the appropriate values
	DataChunk &chunk;
	// Pointers to quickly jump into the right child lineage data
	vector<shared_ptr<LineageDataWithOffset>> &child_ptrs;
	// Returned join chunk to be pushed into chunk scan
	DataChunk &join_chunk;
	// For when we overflow the chunk ex: aggregations with more than 1024 values
	vector<Vector> &cached_values_arr;
	// For when we overflow the chunk with ptrs ex: simple aggs
	vector<vector<shared_ptr<LineageDataWithOffset>>> &cached_child_ptrs_arr;
	// For when we overflow the chunk - the count
	idx_t &overflow_count;
};

// Adapted from https://github.com/roger-dv/cpp20-coro-generator/blob/master/generator.h
template<typename T>
class Generator {
public:
	struct promise_type;
	using handle_type = std::experimental::coroutine_handle<promise_type>;
private:
	handle_type coro;
public:
	explicit Generator(handle_type h) : coro(h) {}
	Generator(const Generator &) = delete;
	Generator(Generator &&oth) noexcept : coro(oth.coro) {
		oth.coro = nullptr;
	}
	Generator &operator=(const Generator &) = delete;
	Generator &operator=(Generator &&other) noexcept {
		coro = other.coro;
		other.coro = nullptr;
		return *this;
	}
	~Generator() {
		if (coro) {
			coro.destroy();
		}
	}

	bool Next() {
		coro.resume();
		return not coro.done();
	}

	T GetValue() {
		return move(coro.promise().current_value);
	}

	struct promise_type {
	private:
		T current_value{};
		friend class Generator;
	public:
		promise_type() = default;
		~promise_type() = default;
		promise_type(const promise_type&) = delete;
		promise_type(promise_type&&) = delete;
		promise_type &operator=(const promise_type&) = delete;
		promise_type &operator=(promise_type&&) = delete;

		auto initial_suspend() {
			return std::experimental::suspend_always{};
		}

		auto final_suspend() noexcept {
			return std::experimental::suspend_always{};
		}

		auto get_return_object() {
			return Generator {handle_type::from_promise(*this)};
		}

		auto return_void() {
			return std::experimental::suspend_never{};
		}

		auto yield_value(T some_value) {
			current_value = move(some_value);
			return std::experimental::suspend_always{};
		}

		void unhandled_exception() {
			std::exit(1);
		}
	};
};

class LineageRes {
public:
	virtual vector<idx_t> GetValues() = 0;
	virtual idx_t GetCount() = 0;
	virtual ~LineageRes() {};
};

class LineageResJoin : public LineageRes {
public:
	LineageResJoin(Generator<unique_ptr<LineageRes>> left_gen, Generator<unique_ptr<LineageRes>> right_gen)
	    : left_gen(move(left_gen)), right_gen(move(right_gen)) {}

	vector<idx_t> GetValues() override;
	idx_t GetCount() override;

private:
	Generator<unique_ptr<LineageRes>> left_gen;
	Generator<unique_ptr<LineageRes>> right_gen;
};

class LineageResVal : public LineageRes {
public:
	explicit LineageResVal(shared_ptr<vector<SourceAndMaybeData>> lineage) : vals(move(lineage)) {}

	vector<idx_t> GetValues() override;
	idx_t GetCount() override;

private:
	shared_ptr<vector<SourceAndMaybeData>> vals;
};

} // namespace duckdb
#endif
