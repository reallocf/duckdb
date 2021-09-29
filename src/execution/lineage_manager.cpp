#ifdef LINEAGE
#include "duckdb/execution/lineage_context.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {
class PhysicalOperator;
class LineageContext;


void ManageLineage::Reset() {
  /* starting a new query */
  pipelines_lineage.clear();
  op_id = 0;
}

/*
 * Persist lineage in-memory for normal execution path
 * */
void ManageLineage::AddOutputLineage(PhysicalOperator* opKey, shared_ptr<LineageContext> lineage) {
  if (lineage->isEmpty() == false) {
    pipelines_lineage[0][opKey].push_back(move(lineage));
  }
}

/*
 * Persist lineage in-memory for Sink based operators
 * */
void ManageLineage::AddLocalSinkLineage(PhysicalOperator* sink,  vector<shared_ptr<LineageContext>> lineage_vec) {
  if (lineage_vec.size() > 0) {
    pipelines_lineage[1][sink].reserve(pipelines_lineage[1][sink].size() + lineage_vec.size());
    pipelines_lineage[1][sink].insert(pipelines_lineage[1][sink].end(), lineage_vec.begin(), lineage_vec.end());
  }
}

/*
 * For each operator in the plan, give it an ID. If there are
 * two operators with the same type, give them a unique ID starting
 * from the zero and incrementing it for the lowest levels of the tree
 */
void ManageLineage::AnnotatePlan(PhysicalOperator *op) {
  if (!op) return;
  op->id = op_id++;
#ifdef LINEAGE_DEBUG
  std::cout << op->GetName() << " " << op->id << std::endl;
#endif
  for (idx_t i = 0; i < op->children.size(); ++i)
    AnnotatePlan(op->children[i].get());
}

void ManageLineage::BackwardLineage(PhysicalOperator *op, shared_ptr<LineageContext> lineage, int oidx) {
  std::cout << "Backward Lineage: TraverseTree op " << op << " " << op->GetName() << std::endl;
  switch (op->type) {
    case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
    case PhysicalOperatorType::HASH_GROUP_BY: {
      std::shared_ptr<LineageOpUnary> lop = std::dynamic_pointer_cast<LineageOpUnary>(lineage->GetLineageOp(op->id, 0));
      if (!lop) {
          std::cout << "something is wrong, lop not found for aggregate" << std::endl;
          return;
      }

      // schema: [oidx idx_t, group idx_t]
      //         maps a row in the output to a group
      idx_t group =  dynamic_cast<LineageDataArray<sel_t>&>(*lop->data).getAtIndex(oidx);
      std::cout << oidx << " belong to " << group << std::endl;

      // Lookup the data on the build side
      if (pipelines_lineage[1].find(op) != pipelines_lineage[1].end()) {
          vector<shared_ptr<LineageContext>> sink_lineage = pipelines_lineage[1][op];
          vector<idx_t> matches;
          for (idx_t i = 0; i < sink_lineage.size(); ++i) {
            std::shared_ptr<LineageOpUnary> sink_lop = std::dynamic_pointer_cast<LineageOpUnary>(sink_lineage[i]->GetLineageOp(op->id, 1));

            if (!sink_lop) {
              std::cout << "something is wrong, aggregate sink lop not found" << std::endl;
              continue;
            }

            // schema: [ridx idx_t, group idx_t]
            //         maps input row to a specific group
            // getAllMatches: get all ridx that belong to group, O(n)
            dynamic_cast<LineageSelVec&>(*sink_lop->data).getAllMatches(group, matches);
            std::cout << " getAllMatches for " << group << " has " << matches.size() << std::endl;
          }
        }
        BackwardLineage(op->children[0].get(), lineage, oidx);
        break;
    } default: {
      for (idx_t i = 0; i < op->children.size(); ++i)
        BackwardLineage(op->children[i].get(), lineage, oidx);
    }
  }
}

} // namespace duckdb
#endif
