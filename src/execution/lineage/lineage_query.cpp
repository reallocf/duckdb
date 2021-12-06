#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_query.hpp"

namespace duckdb {

void fill_complex_first_child(query &first_child, query &Q, string join_type, idx_t child_2_id) {
	if (!Q.in_select.empty())  Q.in_select += ", ";
	if (!first_child.from.empty()) {
		Q.from += join_type;
		if (!first_child.in_select.empty() && !first_child.out_select.empty()) {
			Q.from += "(select "+first_child.in_select+", "
			                 +first_child.out_select+" as temp"+to_string(child_2_id)+"_out_index from "
			                 +first_child.from;
			if (!first_child.condition.empty()) Q.from += " where " +first_child.condition;
			Q.from += ") as temp_"+to_string(child_2_id) + " on (";
			Q.from +=  Q.scan + ".rhs_index=temp_"+to_string(child_2_id)+".temp"+to_string(child_2_id)+"_out_index)";
			Q.in_select +=  "temp_"+to_string(child_2_id)+".*";
		} else {
			Q.in_select += first_child.in_select;
			Q.from += first_child.from + " on (";
			Q.from +=  Q.scan + ".rhs_index="+first_child.scan+".out_index)";
		}
	} else if (!first_child.base_table.empty()) {
		Q.in_select += Q.scan + ".rhs_index as " + first_child.base_table;
	} else {
		Q.in_select += Q.scan + ".rhs_index as " + Q.scan + "_rowid";
	}
}

void fill_complex_second_child(query &second_child, query &Q, string join_type, idx_t child_1_id) {
	if (!Q.in_select.empty())  Q.in_select += ", ";
	if (!second_child.from.empty()) {
		Q.from += join_type;
		if (!second_child.in_select.empty() && !second_child.out_select.empty()) {
			Q.from += "(select " + second_child.in_select + ", " + second_child.out_select + " as temp"+to_string(child_1_id)+"_out_index from " +
					  second_child.from;
			if (!second_child.condition.empty()) Q.from += " where " + second_child.condition;
			Q.from += ") as temp_" + to_string(child_1_id) + " on (";
			Q.from += Q.sink + ".in_index=temp_" + to_string(child_1_id) + ".temp"+to_string(child_1_id)+"_out_index)";
			Q.in_select = "temp_" + to_string(child_1_id) + ".*";
		} else {
			Q.from += second_child.from + " on (";
			Q.from += Q.sink + ".in_index=" + second_child.scan + ".out_index)";
			Q.in_select += second_child.in_select;
		}
	} else if (!second_child.base_table.empty()) {
		Q.in_select +=  Q.sink + ".in_index as " + second_child.base_table;
	} else {
		Q.in_select +=  Q.sink + ".in_index as " + Q.sink + "_rowid";
	}
}

void fill_first_child(query &first_child, query &Q) {
	if (!Q.in_select.empty())  Q.in_select += ", ";
	if (!first_child.from.empty()) {
		if (!Q.from.empty()) Q.from +=  ", ";
		Q.from += first_child.from;
		Q.in_select += first_child.in_select;
		// connect this join to its probe side
		if (!Q.condition.empty()) 	Q.condition += " and ";
		Q.condition += Q.scan + ".rhs_index=" + first_child.scan + ".out_index";
		if (!first_child.condition.empty())
			Q.condition +=  " and " + first_child.condition;
	} else if (!first_child.base_table.empty()) {
		Q.in_select += Q.scan + ".rhs_index as " + first_child.base_table;
	} else {
		Q.in_select += Q.scan + ".rhs_index as " + Q.scan + "_rowid";
	}
}

void fill_second_child(query &second_child, query &Q) {
	if (!Q.in_select.empty())  Q.in_select += ", ";
	if (!second_child.from.empty()) {
		if (!Q.from.empty()) Q.from +=  ", ";
		Q.from += second_child.from;
		Q.in_select += second_child.in_select;
		if (!Q.condition.empty()) 	Q.condition += " and ";
		Q.condition +=  Q.sink + ".in_index=" + second_child.scan + ".out_index";
		if (!second_child.condition.empty())
			Q.condition += " and " + second_child.condition;
	} else if (!second_child.base_table.empty()) {
		Q.in_select += Q.sink + ".in_index as " + second_child.base_table;
	} else {
		Q.in_select += Q.sink + ".in_index as " + Q.sink + "_rowid";
	}
}

struct query join_caluse(string table_name, query &first_child, query &second_child, idx_t child_1_id, idx_t child_2_id, JoinType join_type) {
	query Q;
	Q.scan = table_name+"1";
	Q.sink =  table_name+"0";
	Q.out_select = Q.scan+".out_index";

	switch (join_type) {
	case JoinType::ANTI: {
		Q.from = Q.scan;
		fill_first_child(first_child, Q);
		break;
	}
	case JoinType::SEMI:
	case JoinType::INNER: {
		Q.from = Q.scan + ", " + Q.sink;
		// connect build and probe side
		Q.condition = Q.sink + ".out_address=" + Q.scan + ".lhs_address";
		fill_first_child(first_child, Q);
		fill_second_child(second_child, Q);
		break;
	}
	case JoinType::RIGHT: {
		Q.from = Q.sink + " left join "+ Q.scan + " on (";
		Q.from += Q.sink+".out_address="+Q.scan+".lhs_address)";
		fill_complex_first_child(first_child, Q, " left join ", child_2_id);
		fill_second_child(second_child, Q);
		break;
	}
	case JoinType::OUTER: {
		Q.from = Q.sink + " full join "+ Q.scan + " on (";
		Q.from += Q.sink+".out_address="+Q.scan+".lhs_address)";
		fill_complex_first_child(first_child, Q, " full join ", child_2_id);
		fill_complex_second_child(second_child, Q, " full join ", child_1_id);
		break;
	}
	case JoinType::MARK: // fix: if it is not in, then the children will be empty but here we expect result from the children
	case JoinType::SINGLE:
	case JoinType::LEFT: {
		Q.from = Q.scan + " left join "+ Q.sink + " on (";
		Q.from += Q.sink+".out_address="+Q.scan+".lhs_address)";

		fill_complex_second_child(second_child, Q, " left join ", child_1_id);
		fill_first_child(first_child, Q);

		break;
	}
	default:
		throw InternalException("Unhandled join type in JoinHashTable");
	}
	return Q;
}
// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
struct query GetEndToEndQuery(PhysicalOperator *op, idx_t qid) {

	// Example: LINEAGE_1_HASH_JOIN_3_0
	string table_name = "LINEAGE_" + to_string(qid) + "_"
	                    + op->GetName() + "_"; // + to_string(i);
	query Q;
	switch (op->type) {
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
		Q.scan = table_name + "1";
		Q.sink = table_name + "0";
		Q.from = Q.scan + ", " + Q.sink;
		Q.out_select = Q.scan + ".out_index";
		Q.condition = Q.scan+".in_index="+Q.sink+".out_index";
		query child;
		if (op->children.size() > 0)
			child = GetEndToEndQuery(op->children[0].get(), qid);
		if (!child.from.empty()) {
			Q.in_select = child.in_select;
			Q.from += ", "+child.from;
			Q.condition += " and "+child.scan+".out_index="+Q.sink+".in_index";
			if (!child.condition.empty())
				Q.condition += " and " + child.condition;
		} else if (!child.base_table.empty()) {
			Q.in_select = Q.sink+".in_index as "+child.base_table;
		} else {
			Q.in_select = Q.sink+".in_index as "+ Q.sink+"_rowid";
		}

		break;
	}
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::ORDER_BY: {
		Q.scan = table_name + "0";
		Q.from = Q.scan;
		Q.out_select = Q.scan + ".out_index";

		query child;
		if (op->children.size() > 0)
			child = GetEndToEndQuery(op->children[0].get(), qid);
		if (!child.from.empty()) {
			Q.in_select = child.in_select;
			Q.from += ", "+child.from;
			Q.condition += child.scan+".out_index="+Q.scan+".in_index";
			if (!child.condition.empty())
				Q.condition += " and " + child.condition;
		} else if (!child.base_table.empty()) {
			Q.in_select = Q.scan+".in_index as "+child.base_table;
		} else {
			Q.in_select = Q.scan+".in_index as "+ Q.scan+"_rowid";
		}

		break;
	}
	case PhysicalOperatorType::DELIM_SCAN: {
		Q.base_table = "delimscan_rowid_"+ to_string(op->id);
		break;
	}
	case PhysicalOperatorType::CHUNK_SCAN: {
		Q.base_table = "chunkscan_rowid_"+ to_string(op->id);
		break;
	}
	case PhysicalOperatorType::TABLE_SCAN: {
		string name = dynamic_cast<PhysicalTableScan *>(op)->function.to_string(dynamic_cast<PhysicalTableScan *>(op)->bind_data.get());
		Q.base_table = name+"_rowid_"+ to_string(op->id);
		if (!dynamic_cast<PhysicalTableScan *>(op)->table_filters)
			return Q;

		Q.scan = table_name + "0";
		Q.from = Q.scan;
		Q.in_select = Q.scan + ".in_index as "+Q.base_table;
		Q.out_select = Q.scan + ".out_index";
		break;
	}
	case PhysicalOperatorType::INDEX_JOIN: {
		// index join pull from child[0]
		Q.scan = table_name+"0";
		Q.from = Q.scan;
		Q.out_select = Q.scan+".out_index";

		query first_child = GetEndToEndQuery(op->children[0].get(), qid);
		query second_child = GetEndToEndQuery(op->children[1].get(), qid);
		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select = first_child.in_select;
			Q.condition += Q.scan+".rhs_index="+first_child.scan+".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select = Q.scan+".rhs_index as "+first_child.base_table;
		} else {
			Q.in_select = Q.scan+".rhs_index as "+Q.scan+"_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition += " and " + first_child.condition;
		Q.in_select += ", " + Q.scan+".lhs_index as "+second_child.base_table;
		break;
	}
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		Q.scan = table_name+"0";
		Q.from = Q.scan;
		Q.out_select = Q.scan+".out_index";

		query first_child = GetEndToEndQuery(op->children[0].get(), qid);
		query second_child = GetEndToEndQuery(op->children[1].get(), qid);
		if (!first_child.from.empty()) {
			Q.from += ", " + first_child.from;
			Q.in_select = first_child.in_select;
			Q.condition += Q.scan+".rhs_index="+first_child.scan+".out_index";
		} else if (!first_child.base_table.empty()) {
			Q.in_select += Q.scan+".rhs_index as "+first_child.base_table;
		} else {
			Q.in_select = Q.scan+".rhs_index as "+Q.scan+"_rowid";
		}
		if (!first_child.condition.empty())
			Q.condition += " and " + first_child.condition;

		if (!second_child.from.empty()) {
			Q.from += ", " + second_child.from;
			Q.in_select += ", " + second_child.in_select;
			if (!Q.condition.empty())
				Q.condition += " and ";
			Q.condition += Q.scan+".lhs_index="+second_child.scan+".out_index";
		} else if (!second_child.base_table.empty()) {
			Q.in_select += ", "+Q.scan+".lhs_index as "+second_child.base_table;
		} else {
			Q.in_select += ", "+Q.scan+".lhs_index as "+Q.scan+"_rowid";
		}
		if (!second_child.condition.empty())
			Q.condition += " and " + second_child.condition;
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
		// 0->left, 1->right, but since we flipped it it is the opposite for now
		query first_child = GetEndToEndQuery(op->children[0].get(), qid);
		query second_child = GetEndToEndQuery(op->children[1].get(), qid);
		Q = join_caluse(table_name,first_child, second_child, op->children[0]->id, op->children[1]->id, dynamic_cast<PhysicalJoin *>(op)->join_type);
		break;
	}
	case PhysicalOperatorType::SIMPLE_AGGREGATE: {
		Q = GetEndToEndQuery(op->children[0].get(), qid);
		Q.scan = "temp_opio"+ to_string(op->id);
		Q.from = "(SELECT "+ Q.in_select+", 0 as out_index from "+Q.from;
		if (!Q.condition.empty())
			Q.from +=" where "+Q.condition;
		Q.from += +") as "+ Q.scan;
		Q.in_select = Q.scan+".*";
		Q.out_select = "0 as out_index";
		Q.condition = "";
		break;
	}
	case PhysicalOperatorType::DELIM_JOIN: {
		// delim scan, group the values from the delim join probe side
		std::cout <<((PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get())->ToString() << std::endl;
		query Q_distinct = GetEndToEndQuery( (PhysicalOperator *)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), qid);

		// the build side
		std::cout <<dynamic_cast<PhysicalDelimJoin *>(op)->join->ToString() << std::endl;
		query join_sink = GetEndToEndQuery(dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->children[1].get(), qid);

		// delim join probe side
		query child = GetEndToEndQuery(op->children[0].get(), qid);

		string join_table_name =  "LINEAGE_"+ to_string(qid)+"_"+dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->GetName()+"_";
		JoinType join_type = dynamic_cast<PhysicalJoin *>(dynamic_cast<PhysicalDelimJoin *>(op)->join.get())->join_type;
		Q = join_caluse(join_table_name, child, join_sink, op->children[0]->id, dynamic_cast<PhysicalDelimJoin *>(op)->join.get()->children[1].get()->id, join_type);
		break;
	}
	case PhysicalOperatorType::PROJECTION: {
		Q = GetEndToEndQuery(op->children[0].get(), qid);
	}
	default: {

	}
	}

	return Q;
}

} // namespace duckdb
#endif
