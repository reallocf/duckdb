#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/execution/expression_executor.hpp"


#ifdef LINEAGE
#include "duckdb/parallel/task_context.hpp"
#endif

namespace duckdb {

class PhysicalProjectionState : public PhysicalOperatorState {
public:
	PhysicalProjectionState(PhysicalOperator &op, PhysicalOperator *child, vector<unique_ptr<Expression>> &expressions)
	    : PhysicalOperatorState(op, child), executor(expressions) {
		D_ASSERT(child);
	}

	ExpressionExecutor executor;
};

void PhysicalProjection::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                          PhysicalOperatorState *state_p) const {
	auto state = reinterpret_cast<PhysicalProjectionState *>(state_p);

	// get the next chunk from the child
	children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
	if (state->child_chunk.size() == 0) {
		return;
	}

	state->executor.Execute(state->child_chunk, chunk);

#ifdef LINEAGE
	lineage_op.at(context.task.thread_id)->chunk_collection.Append(chunk);
#endif
}

unique_ptr<PhysicalOperatorState> PhysicalProjection::GetOperatorState() {
	return make_unique<PhysicalProjectionState>(*this, children[0].get(), select_list);
}

void PhysicalProjection::FinalizeOperatorState(PhysicalOperatorState &state_p, ExecutionContext &context) {
	auto &state = reinterpret_cast<PhysicalProjectionState &>(state_p);
	context.thread.profiler.Flush(this, &state.executor, "projection", 0);
	if (!children.empty() && state.child_state) {
		children[0]->FinalizeOperatorState(*state.child_state, context);
	}
}

string PhysicalProjection::ParamsToString() const {
	string extra_info;
	extra_info += to_string(hasFunction);
	extra_info += "\n[INFOSEPARATOR]\n";
	for (auto &expr : select_list) {
		string col =  expr->ToString() + "#DEL#" +  expr->GetColumnBindings();
		extra_info += col + "\n";
	}
	return extra_info;
}

} // namespace duckdb
