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
#include <utility>


namespace duckdb {
class PhysicalOperator;
class ClientContext;
class LineageContext;


class ManageLineage {
public:
	explicit ManageLineage(ClientContext &context) : context(context), query_id(0){};

	void AnnotatePlan(PhysicalOperator *op);
	void CreateQueryTable();
	void LogQuery(const string& input_query);
	void CreateLineageTables(PhysicalOperator *op);

	ClientContext &context;
	idx_t query_id = 0;
	string query_list_table_name = "queries_list";
};


class LineageData {
public:
	LineageData() {}
	virtual idx_t Count() = 0;
	virtual idx_t Size() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(idx_t count_so_far) = 0;
	virtual ~LineageData() {};
};

// TODO get templating working like before - that would be better

class LineageDataRowVector : public LineageData {
public:
	LineageDataRowVector(vector<row_t> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	void Debug() override {
		std::cout << "LineageDataVector " << " " << typeid(vec).name() << std::endl;
		for (idx_t i = 0; i < count; i++) {
			std::cout << " (" << i << " -> " << vec[i] << ") ";
		}
		std::cout << std::endl;
	}

	idx_t Size() override {
		return count * sizeof(vec[0]);
	}

	idx_t Count() override {
		return count;
	}

	data_ptr_t Process(idx_t count_so_far) override {
		return (data_ptr_t)vec.data();
	}

	vector<row_t> vec;
	idx_t count;
};


class LineageDataUIntPtrArray : public LineageData {
public:
	LineageDataUIntPtrArray(unique_ptr<uintptr_t[]> vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	void Debug() override {
		std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
		for (idx_t i = 0; i < count; i++) {
			std::cout << " (" << i << " -> " << vec[i] << ") ";
		}
		std::cout << std::endl;
	}

	idx_t Size() override {
		return count * sizeof(vec[0]);
	}

	idx_t Count() override {
		return count;
	}

	data_ptr_t Process(idx_t count_so_far) override {
		return (data_ptr_t)vec.get();
	}

	unique_ptr<uintptr_t[]> vec;
	idx_t count;
};


class LineageDataUInt32Array : public LineageData {
public:
	LineageDataUInt32Array(unique_ptr<uint32_t[]>vec_p, idx_t count) : vec(move(vec_p)), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	void Debug() override {
		std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
		for (idx_t i = 0; i < count; i++) {
			std::cout << " (" << i << " -> " << vec[i] << ") ";
		}
		std::cout << std::endl;
	}

	idx_t Size() override {
		return count * sizeof(vec[0]);
	}

	idx_t Count() override {
		return count;
	}

	data_ptr_t Process(idx_t count_so_far) override {
		return (data_ptr_t)vec.get();
	}

	unique_ptr<uint32_t[]> vec;
	idx_t count;
};


class LineageSelVec : public LineageData {
public:
	LineageSelVec(const SelectionVector& vec_p, idx_t count) : vec(vec_p), count(count) {
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	void Debug() override {
		std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
		for (idx_t i = 0; i < count; i++) {
			std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] << ") ";
		}
		std::cout << std::endl;
	}

	idx_t Size() override {
		return count * sizeof(vec.get_index(0));
	}

	idx_t Count() override {
		return count;
	}

	data_ptr_t Process(idx_t count_so_far) override {
		return (data_ptr_t)vec.data();
	}

	SelectionVector vec;
	idx_t count;
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

	void Debug() override {
		std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
	}

	idx_t Size() override {
	  return 2*sizeof(start);
	}

	idx_t Count() override {
		return end - start;
	}

	data_ptr_t Process(idx_t count_so_far) override {
		// Lazily convert lineage range to selection vector
		if (vec.empty()) {
			for (idx_t i = start; i < end; i++) {
				vec.push_back(count_so_far + i);
			}
		}
		return (data_ptr_t)vec.data();
	}

	idx_t start;
	idx_t end;
	vector<sel_t> vec;
};

// Captures two lineage data of the same side - used for Joins
class LineageBinaryData : public LineageData {
public:
	LineageBinaryData(unique_ptr<LineageData> lhs, unique_ptr<LineageData> rhs) :
	      left(std::move(lhs)), right(std::move(rhs)) {
		D_ASSERT(left->Count() == right->Count());
#ifdef LINEAGE_DEBUG
		Debug();
#endif
	}

	void Debug() override {
		left->Debug();
		right->Debug();
	}

	idx_t Size() override {
		return left->Size() + right->Size();
	}

	idx_t Count() override {
		return left->Count();
	}

	data_ptr_t Process(idx_t count_so_far) override {
		if (switch_on_left) {
			switch_on_left = !switch_on_left;
			return left->Process(count_so_far);
		} else {
			switch_on_left = !switch_on_left;
			return right->Process(count_so_far);
		}
	}

	unique_ptr<LineageData> left;
	unique_ptr<LineageData> right;
	bool switch_on_left = true;
};

#ifndef LINEAGE_UNARY

#define LINEAGE_UNARY 0
#define LINEAGE_SINK 0
#define LINEAGE_SOURCE 1
#define LINEAGE_BUILD 0
#define LINEAGE_PROBE 1

#endif

struct LineageProcessStruct {
	idx_t count_so_far;
	bool still_processing;
};

class LineageOp {
public:
	LineageOp()  {}

	idx_t Size() {
		idx_t size = 0;
		for (const auto& lineage_data : data[0]) {
			size += lineage_data->Size();
		}
		for (const auto& lineage_data : data[1]) {
			size += lineage_data->Size();
		}
		return size;
	};

	void Capture(const shared_ptr<LineageData>& datum, idx_t lineage_idx) {
		data[lineage_idx].push_back(datum);
	}

	void FinishedProcessing() {
		finished_idx++;
		data_idx = 0;
	}

	LineageProcessStruct Process(const vector<LogicalType>& types, idx_t count_so_far, DataChunk &insert_chunk) {
		bool still_processing = true;
		if (data[1].empty()) {
			// Non-Pipeline Breaker
			if (data[0].size() <= data_idx) {
				still_processing = false;
			} else {
				if (dynamic_cast<LineageBinaryData*>(data[LINEAGE_UNARY][0].get()) != nullptr) {
					// Index Join
					// schema: [INTEGER lhs_index, BIGINT rhs_index, INTEGER out_index]

					idx_t res_count = data[0][data_idx]->Count();

					Vector lhs_payload(types[0], data[0][data_idx]->Process(count_so_far));
					Vector rhs_payload(types[1], data[0][data_idx]->Process(count_so_far));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Reference(lhs_payload);
					insert_chunk.data[1].Reference(rhs_payload);
					insert_chunk.data[2].Sequence(count_so_far, 1);
					count_so_far += res_count;
				} else {
					// Seq Scan, Filter, Limit, etc...
					// schema: [INTEGER in_index, INTEGER out_index]

					idx_t res_count = data[0][data_idx]->Count();

					Vector payload(types[0], data[0][data_idx]->Process(count_so_far));

					insert_chunk.SetCardinality(res_count);
					insert_chunk.data[0].Reference(payload);
					insert_chunk.data[1].Sequence(count_so_far, 1);
					count_so_far += res_count;
				}
			}
		} else {
			// Pipeline Breaker
			if (data[finished_idx].size() <= data_idx) {
				still_processing = false;
			} else {
				if (dynamic_cast<LineageBinaryData*>(data[LINEAGE_PROBE][0].get()) != nullptr) {
					// Hash Join - other joins too?
					if (finished_idx == 0) {
						// schema1: [INTEGER in_index, INTEGER out_address] TODO remove this one now that no chunking?

						idx_t res_count = data[0][data_idx]->Count();

						Vector payload(types[1], data[0][data_idx]->Process(count_so_far));

						insert_chunk.SetCardinality(res_count);
						insert_chunk.data[0].Sequence(count_so_far, 1);
						insert_chunk.data[1].Reference(payload);
						count_so_far += res_count;
					} else {
						// schema2: [INTEGER lhs_address, INTEGER rhs_index, INTEGER out_index]

						idx_t res_count = data[1][data_idx]->Count();

						Vector lhs_payload(types[0], data[1][data_idx]->Process(count_so_far));
						Vector rhs_payload(types[1], data[1][data_idx]->Process(count_so_far));

						insert_chunk.SetCardinality(res_count);
						insert_chunk.data[0].Reference(lhs_payload);
						insert_chunk.data[1].Reference(rhs_payload);
						insert_chunk.data[2].Sequence(count_so_far, 1);
						count_so_far += res_count;
					}
				} else {
					// Hash Aggregate / Perfect Hash Aggregate
					// schema for both: [INTEGER in_index, INTEGER out_index]
					if (finished_idx == 0) {
						idx_t res_count = data[finished_idx][data_idx]->Count();

						Vector payload(types[1], data[finished_idx][data_idx]->Process(count_so_far));

						insert_chunk.SetCardinality(res_count);
						insert_chunk.data[0].Sequence(count_so_far, 1);
						insert_chunk.data[1].Reference(payload);
						count_so_far += res_count;
					} else {
						// TODO: can we remove this one for Hash Aggregate?
						idx_t res_count = data[finished_idx][data_idx]->Count();

						Vector payload(types[0], data[finished_idx][data_idx]->Process(count_so_far));

						insert_chunk.SetCardinality(res_count);
						insert_chunk.data[0].Reference(payload);
						insert_chunk.data[1].Sequence(count_so_far, 1);
						count_so_far += res_count;
					}
				}
			}
		}
		data_idx++;
		return LineageProcessStruct{
		    count_so_far,
		    still_processing
		};
	}

	// data[0] used by all ops; data[1] used by pipeline breakers
	// TODO does this need to have a shared_ptr wrapper?
	std::vector<shared_ptr<LineageData>> data[2];
	idx_t finished_idx = 0;
	idx_t data_idx = 0;
};

} // namespace duckdb
#endif
