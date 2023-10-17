#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_manager.hpp"

#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include <utility>

namespace duckdb {
class PhysicalDelimJoin;
class PhysicalJoin;
class PhysicalTableScan;
class PhysicalProjection;

void fillBaseChunk(DataChunk &insert_chunk, idx_t res_count, Vector &lhs_payload,
    Vector &rhs_payload, idx_t count_so_far, int thread_id) {
	insert_chunk.SetCardinality(res_count);
	insert_chunk.data[0].Reference(lhs_payload);
	insert_chunk.data[1].Reference(rhs_payload);
	insert_chunk.data[2].Sequence(count_so_far, 1);
}

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<vector<ColumnDefinition>> LineageManager::GetTableColumnTypes(PhysicalOperator *op) {
	vector<vector<ColumnDefinition>> res;
	switch (op->type) {
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN:
	case PhysicalOperatorType::ORDER_BY: {
		// schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> source;
		source.emplace_back("in_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		// source schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
		vector<ColumnDefinition> source;
		source.emplace_back("in_index", LogicalType::UBIGINT);
		source.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source));
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: 
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT: {
		// schema: [INTEGER lhs_index, INTEGER|BIGINT rhs_index, INTEGER out_index]
		vector<ColumnDefinition> source;
		source.emplace_back("lhs_index", LogicalType::INTEGER);
		source.emplace_back("rhs_index", LogicalType::INTEGER);
		source.emplace_back("out_index", LogicalType::INTEGER);
		res.emplace_back(move(source));
    break;
  }
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return res;
}


//! Construct empty tables for operators lineage to be accessed using Lineage Scan
void LineageManager::CreateLineageTables(PhysicalOperator *op, idx_t query_id) {
	vector<vector<ColumnDefinition>> table_column_types = GetTableColumnTypes(op);
	for (idx_t i = 0; i < table_column_types.size(); i++) {
		// Example: LINEAGE_1_HASH_JOIN_3_0
		string table_name = "LINEAGE_" + to_string(query_id) + "_"
		                    + op->GetName() + "_" + to_string(i);
		// Create Table
		auto info = make_unique<CreateTableInfo>(DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			info->columns.push_back(move(table_column_types[i][col_i]));
		}

		table_lineage_op[table_name] = op->lineage_op;

		// add column_stats, cardinality
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		TableCatalogEntry* table = (TableCatalogEntry*)catalog.CreateTable(context, bound_create_info.get());
		table->storage->info->cardinality = 100;
		table->storage->UpdateStats();
	}

	// persist intermediate values
	if (persist_intermediate) {
		vector<ColumnDefinition> table;
		for (idx_t col_i = 0; col_i < op->types.size(); col_i++) {
			table.emplace_back("col_" + to_string(col_i), op->types[col_i]);
		}

		string table_name = "LINEAGE_" + to_string(query_id) + "_"  + op->GetName() + "_100";
		auto info = make_unique<CreateTableInfo>(DEFAULT_SCHEMA, table_name);
		for (idx_t col_i = 0; col_i < table.size(); col_i++) {
			info->columns.push_back(move(table[col_i]));
		}
		auto binder = Binder::CreateBinder(context);
		auto bound_create_info = binder->BindCreateTableInfo(move(info));
		auto &catalog = Catalog::GetCatalog(context);
		catalog.CreateTable(context, bound_create_info.get());
		table_lineage_op[table_name] = op->lineage_op;
	}

	if (op->type == PhysicalOperatorType::DELIM_JOIN) {
		CreateLineageTables( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), query_id);
		CreateLineageTables( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), query_id);
	}

	// If the operator is unimplemented or doesn't materialize any lineage, it'll be skipped and we'll just
	// iterate through its children
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(op->children[i].get(), query_id);
	}
}

void  getchunk(const vector<LogicalType>& types, idx_t res_count, idx_t count_so_far,
              DataChunk &insert_chunk, int thread_id, data_ptr_t ptr) {
  insert_chunk.Reset();
  insert_chunk.SetCardinality(res_count);
  if (ptr != nullptr) {
    Vector in_index(LogicalType::INTEGER, ptr); // TODO: add offset
    insert_chunk.data[0].Reference(in_index);
  } else {
    insert_chunk.data[0].Sequence(count_so_far, 1); // out_index
  }
  insert_chunk.data[1].Sequence(count_so_far, 1); // out_index
}

LineageProcessStruct TableScanLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t start = lineage[data_idx].start;
  idx_t vector_index = lineage[data_idx].vector_index;
  idx_t child_offset = start + vector_index * STANDARD_VECTOR_SIZE;
  data_ptr_t ptr = nullptr;
  if (lineage[data_idx].sel != nullptr) {
    auto vec_ptr = lineage[data_idx].sel->owned_data.get();
    for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += child_offset;
		}
    ptr = (data_ptr_t)vec_ptr;
  }
  getchunk(types, res_count, count_so_far, insert_chunk, thread_id, ptr);

  //std::cout << insert_chunk.ToString() << std::endl;
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}


LineageProcessStruct FilterLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t child_offset = lineage[data_idx].child_offset;
  data_ptr_t ptr = nullptr;
  if (lineage[data_idx].sel != nullptr) {
    auto vec_ptr = lineage[data_idx].sel.get();
    for (idx_t i = 0; i < res_count; i++) {
			*(vec_ptr + i) += child_offset;
		}
    ptr = (data_ptr_t)vec_ptr;
  }
  getchunk(types, res_count, count_so_far, insert_chunk, thread_id, ptr);
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}

LineageProcessStruct LimitLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
}


LineageProcessStruct OrderByLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
}

LineageProcessStruct HashJoinLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage_binary.size() ) {
    return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }

  idx_t lsn = output_index[data_idx];
  
  idx_t res_count = lineage_binary[lsn].count;
  idx_t out_offset = lineage_binary[lsn].out_offset;
  data_ptr_t left_ptr = (data_ptr_t)lineage_binary[lsn].left.get();
  data_ptr_t right_ptr;
  uint64_t* right_build_ptr = (uint64_t*)lineage_binary[lsn].right.get();

  Vector lhs_payload(LogicalType::INTEGER);
	Vector rhs_payload(LogicalType::INTEGER);
  
  // Left side / probe side
  if (left_ptr == nullptr) {
    if (res_count == STANDARD_VECTOR_SIZE) {
      lhs_payload.Sequence(count_so_far, 1); // out_index
    } else {
      lhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
      ConstantVector::SetNull(lhs_payload, true);
    }
  } else {
    Vector temp(LogicalType::INTEGER, left_ptr);
    lhs_payload.Reference(temp);
  }

  // Right side / build side
  if (right_build_ptr == nullptr) {
    rhs_payload.SetVectorType(VectorType::CONSTANT_VECTOR);
    ConstantVector::SetNull(rhs_payload, true);
  } else {
    if (right_val_log.size() < (lsn+1)) {
      SelectionVector right_val(res_count);
      for (idx_t i=0; i < res_count; i++) {
        idx_t cur = (idx_t)right_build_ptr[i];
        for (idx_t it = 0; it < hm_range.size(); ++it) {
          //std::cout << cur << " " << hm_range[it].first << " " << hm_range[it].second << " " << offset << " " << hash_chunk_count[it] <<  std::endl;
          if (cur >= hm_range[it].first && cur <= hm_range[it].second) {
            idx_t val = ((cur - hm_range[it].first) / offset) + hash_chunk_count[it];
            //std::cout << i << " " << val << std::endl;
            right_val.set_index(i, val);
          }
        }
      }
      right_val_log.push_back(move(right_val.sel_data()->owned_data));
      right_ptr = (data_ptr_t)right_val_log.back().get();
    } else {
      right_ptr = (data_ptr_t)right_val_log[lsn].get();
    } 

    Vector temp(LogicalType::INTEGER, (data_ptr_t)right_ptr);
    rhs_payload.Reference(temp);
  }

  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);

  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage_binary.size() > data_idx };
}

LineageProcessStruct IndexJoinLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t out_start = lineage[data_idx].child_offset;

  data_ptr_t left_ptr = (data_ptr_t)lineage[data_idx].left.data();
  data_ptr_t right_ptr = (data_ptr_t)lineage[data_idx].right.data();

  Vector lhs_payload(LogicalType::INTEGER, left_ptr);
  Vector rhs_payload(LogicalType::INTEGER, right_ptr);
  
  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);
  
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}

LineageProcessStruct CrossLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].left_chunk;
  idx_t out_start = lineage[data_idx].out_start;
  idx_t right_position = lineage[data_idx].right_position;

  Vector rhs_payload(Value::Value::INTEGER(right_position));
  Vector lhs_payload(LogicalType::INTEGER, res_count);
  lhs_payload.Sequence(out_start, 1);
  
  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);
  
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}

LineageProcessStruct NLJLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t out_start = lineage[data_idx].out_start;

  data_ptr_t left_ptr = (data_ptr_t)lineage[data_idx].left.data();
  data_ptr_t right_ptr = (data_ptr_t)lineage[data_idx].right.data();

  Vector lhs_payload(LogicalType::INTEGER, left_ptr);
  Vector rhs_payload(LogicalType::INTEGER, right_ptr);
  
  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);
  
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}

LineageProcessStruct BNLJLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t out_start = lineage[data_idx].out_start;
  idx_t left_position = lineage[data_idx].left_position;

  Vector lhs_payload(Value::Value::INTEGER(left_position));
  data_ptr_t right_ptr = (data_ptr_t)lineage[data_idx].match_sel.data();
  Vector rhs_payload(LogicalType::INTEGER, right_ptr);
  
  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);
  
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}


LineageProcessStruct MergeLineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  if (data_idx >= lineage.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }
    
  idx_t res_count = lineage[data_idx].count;
  idx_t out_start = lineage[data_idx].out_start;

  data_ptr_t left_ptr = (data_ptr_t)lineage[data_idx].left.data();
  data_ptr_t right_ptr = (data_ptr_t)lineage[data_idx].right.data();

  Vector lhs_payload(LogicalType::INTEGER, left_ptr);
  Vector rhs_payload(LogicalType::INTEGER, right_ptr);
  
  fillBaseChunk(insert_chunk, res_count, lhs_payload, rhs_payload, count_so_far, thread_id);
  
  count_so_far += res_count;
  data_idx++;
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, lineage.size() > data_idx };
}

LineageProcessStruct PHALineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]
  
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
}

LineageProcessStruct HALineage::GetLineageAsChunk(const vector<LogicalType>& types,
  idx_t count_so_far, DataChunk &insert_chunk, idx_t size, int thread_id,
  idx_t data_idx, idx_t stage_idx) {
  // schema: [INTEGER in_index, INTEGER out_index, INTEGER thread_id]

  if (data_idx < scan_log.size() && current_key >= scan_log[data_idx].count) {
    data_idx++;
    current_key = 0;
  }

  if (data_idx >= scan_log.size()) {
	  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, false };
  }

  // in_index|LogicalType::BIGINT, out_index|LogicalType::INTEGER, thread_id| LogicalType::INTEGER

  idx_t scan_count = scan_log[data_idx].count;
  auto payload = scan_log[data_idx].addchunk_lineage.get();
  idx_t output_key = (idx_t)payload[current_key];
  // current scan , current offset into scan, current offset into groups of scan
  vector<idx_t>& la = hash_map_agg[output_key];
  // read from offset_within_key to max(1024, la.size());
  idx_t end_offset = la.size() - offset_within_key;
  if (end_offset > STANDARD_VECTOR_SIZE) {
    end_offset = STANDARD_VECTOR_SIZE;
  }
	insert_chunk.SetCardinality(end_offset);

  // TODO: only display between offset_within_key to end_offset
  data_ptr_t ptr = (data_ptr_t)(la.data() + offset_within_key);
  Vector in_index(LogicalType::UBIGINT, ptr);
  // use in index to loop up hash_map_agg
	insert_chunk.data[0].Reference(in_index);
  insert_chunk.data[1].Reference(Value::INTEGER(current_key)); // out_index

	count_so_far += end_offset;
  offset_within_key += end_offset;
  if (offset_within_key >= la.size()) {
    offset_within_key = 0;
    current_key++;
  }
  return LineageProcessStruct{ count_so_far, 0, data_idx, stage_idx, scan_log.size() > data_idx };
}

} // namespace duckdb
#endif
