//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage_context.hpp
//
//
//===----------------------------------------------------------------------===//

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
class LineageContext;


class ManageLineage {
public:
	ManageLineage(ClientContext &context) : context(context)  {
	    query_id  = 0;
		query_table = "queries_list";
        create_table_exists = false;
    };

	void setQuery(string input_query);
    void CreateQueryTable(ClientContext &context);
    void CreateLineageTables(PhysicalOperator *op, ClientContext &context);
	void AnnotatePlan(PhysicalOperator *op);
    void Persist(PhysicalOperator* op, shared_ptr<LineageContext> lineage, ClientContext &context, bool is_sink);
    void BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx, ClientContext &context);
    void ForwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int idx, ClientContext &context);
	void Reset();
    void LineageSize();

    void AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext>  lineage);
    void AddLocalSinkLineage(PhysicalOperator* opKey,  shared_ptr<LineageContext> lineage);

	std::unordered_map<string, int> op_metadata;

    // lineage for tree starting from this operator
    // 0: get chunk
    // 1: sink
    unordered_map<int, unordered_map<PhysicalOperator*, vector<shared_ptr<LineageContext>>>> pipelines_lineage;
    idx_t query_id;
	string query_table;
    DataChunk insert_chunk;
    ClientContext &context;
    bool create_table_exists;
};

class LineageData {
public:
    LineageData() {}
    virtual unsigned long size_bytes() = 0;
	virtual void debug() = 0;
	virtual void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) = 0;
};

template <class T>
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
    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
        TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
        DataChunk insert_chunk;
        insert_chunk.Initialize(table->GetTypes());
        insert_chunk.SetCardinality(count);

        // map payload to a vector
        Vector payload;
        payload.SetType(table->GetTypes()[0]);
        FlatVector::SetData(payload, (data_ptr_t)&vec[0]);

        // map segment id to a vector
        Vector chunk_ids;
        chunk_ids.SetType(table->GetTypes()[2]);
        chunk_ids.Reference( Value::Value::INTEGER(chunk_id));

        // populate chunk
        insert_chunk.data[0].Reference(payload);
        insert_chunk.data[1].Sequence(seq_offset, 1);
        insert_chunk.data[2].Reference(chunk_ids);

        table->Persist(*table, context, insert_chunk);
    }

    int findIndexOf(idx_t data) {
        for (idx_t i = 0; i < count; i++) {
            if (vec[i] == (T)data) return (int)i;
        }
        return -1;
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

    LineageDataArray (unique_ptr<T[]> vec_p, idx_t count, LogicalType type = LogicalType::INTEGER) :
	      vec(move(vec_p)), count(count), type(type) {
#ifdef LINEAGE_DEBUG
    this->debug();
#endif
    }

	void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
        TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
        DataChunk insert_chunk;
        insert_chunk.Initialize(table->GetTypes());
        insert_chunk.SetCardinality(count);

		// map payload to a vector
        Vector payload;
        payload.SetType(table->GetTypes()[0]);
        FlatVector::SetData(payload, (data_ptr_t)&vec[0]);

        // map segment id to a vector
		Vector chunk_ids;
		chunk_ids.SetType(table->GetTypes()[2]);
        chunk_ids.Reference( Value::Value::INTEGER(chunk_id));

		// populate chunk
        insert_chunk.data[0].Reference(payload);
        insert_chunk.data[1].Sequence(seq_offset, 1);
        insert_chunk.data[2].Reference(chunk_ids);

        table->Persist(*table, context, insert_chunk);
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
	LogicalType type;
};


class LineageSelVec : public LineageData {
public:

    LineageSelVec (SelectionVector vec_p, idx_t count, idx_t offset = 0, LogicalType type = LogicalType::INTEGER) :
        vec(move(vec_p)), count(count), type(type), offset(offset) {
#ifdef LINEAGE_DEBUG
        this->debug();
#endif
    }

    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
        TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
        DataChunk insert_chunk;
        insert_chunk.Initialize(table->GetTypes());
        insert_chunk.SetCardinality(count);

        // map payload to a vector
        Vector payload;
        payload.SetType(table->GetTypes()[0]);
        FlatVector::SetData(payload, (data_ptr_t)&vec.sel_data()->owned_data[0]);

		// map segment id to a vector
        Vector chunk_ids;
        chunk_ids.SetType(table->GetTypes()[2]);
        chunk_ids.Reference( Value::Value::INTEGER(chunk_id));

        // populate chunk
        insert_chunk.data[0].Reference(payload);
        insert_chunk.data[1].Sequence(seq_offset, 1);
        insert_chunk.data[2].Reference(chunk_ids);

        table->Persist(*table, context, insert_chunk);

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

    idx_t findIndexOf(idx_t data) {
        for (idx_t i = 0; i < count; i++) {
            if (vec.get_index(i) == data) return i;
        }
        return 0;
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

// A PassThrough to indicate that the operator doesn't affect lineage at all
// for example in the case of a Projection
class LineagePassThrough : public LineageData {
public:

	LineagePassThrough() {}
	void debug() {}
    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
        std::cout << "persist " << tablename << std::endl;
    }
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
		this.debug();
#endif
	}
    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
        TableCatalogEntry * table = Catalog::GetCatalog(context).GetEntry<TableCatalogEntry>(context,  DEFAULT_SCHEMA, tablename);
        DataChunk insert_chunk;
        insert_chunk.Initialize(table->GetTypes());
        insert_chunk.SetCardinality(1);

        // map payload to a vector
        Vector start_vec;
        start_vec.SetType(table->GetTypes()[0]);
        start_vec.Reference( Value::Value::INTEGER(start));

        // map payload to a vector
        Vector end_vec;
        end_vec.SetType(table->GetTypes()[0]);
        end_vec.Reference( Value::Value::INTEGER(end));


        // map segment id to a vector
        Vector chunk_ids;
        chunk_ids.SetType(table->GetTypes()[2]);
        chunk_ids.Reference( Value::Value::INTEGER(chunk_id));

        // populate chunk
        insert_chunk.data[0].Reference(start_vec);
        insert_chunk.data[1].Reference(end_vec);
        insert_chunk.data[2].Reference(chunk_ids);

        table->Persist(*table, context, insert_chunk);
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
    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
    }
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
    void persist(ClientContext &context, string tablename, int32_t chunk_id, int seq_offset = 0) {
    }

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
        chunk_id = 0;
    }

    void RegisterDataPerOp(PhysicalOperator* key, shared_ptr<LineageOp> op, int type = 0) {
        ht[type][key] = move(op);
    }

	bool isEmpty() {
		return ht.empty();
	}

	shared_ptr<LineageOp> GetLineageOp(PhysicalOperator* key, int type) {
		if (ht.find(type) == ht.end()) {
			return NULL;
		}
		if (ht[type].find(key) == ht[type].end()) {
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
	int32_t chunk_id;
};

} // namespace duckdb
