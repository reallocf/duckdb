#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

// LineageSelVec

void LineageSelVec::Debug() {
	std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] + in_offset << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageSelVec::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		*(vec.data() + i) += offset + in_offset;
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageSelVec::Backward(idx_t source) {
	return (idx_t)vec.sel_data()->owned_data[source]+ in_offset;
}

vector<LineageDataWithOffset> LineageSelVec::Divide() {
	vector<LineageDataWithOffset> res(count / STANDARD_VECTOR_SIZE + 1);
	for (idx_t i = 0; i < count / STANDARD_VECTOR_SIZE + 1; i++) {
		idx_t this_offset = i * STANDARD_VECTOR_SIZE;
		idx_t this_count = STANDARD_VECTOR_SIZE;
		if (this_offset + STANDARD_VECTOR_SIZE > count) {
			this_count = count - this_offset;
		}
		buffer_ptr<SelectionData> this_data = make_buffer<SelectionData>(this_count);
		move(
		    vec.sel_data().get()->owned_data.get() + this_offset,
		    vec.sel_data().get()->owned_data.get() + this_offset + this_count,
		    this_data.get()->owned_data.get()
		);
		auto this_lineage = make_shared<LineageSelVec>(SelectionVector(this_data), this_count);
		this_lineage->SetChild(child);
		res[i] = {this_lineage, (int)this_offset, this_offset}; // I don't think this offset management is correct, but we don't use this anymore so it's fine
	}
	return res;
}

// LineageBinary

idx_t LineageBinary::Count() {
	if (left) return left->Count();
	else return right->Count();
}

void LineageBinary::Debug() {
	if (left) left->Debug();
	if (right) right->Debug();
}

data_ptr_t LineageBinary::Process(idx_t offset) {
	if (switch_on_left && left) {
		switch_on_left = !switch_on_left;
		return left->Process(offset);
	} else if (right) {
		switch_on_left = !switch_on_left;
		return right->Process(offset);
	} else {
		return nullptr;
	}
}

idx_t LineageBinary::Size() {
	auto size = 0;
	if (left) size += left->Size();
	if (right) size += right->Size();
	return size;
}

// LineageNested

void LineageNested::Debug() {
	std::cout << "LineageNested:" << std::endl;
	for (const shared_ptr<LineageDataWithOffset>& lineage_data : lineage) {
		std::cout << "    ";
		lineage_data->data->Debug();
	}
	std::cout << "End LineageNested" << std::endl;
}

void LineageNested::AddLineage(const shared_ptr<LineageDataWithOffset>& lineage_data) {
	count += lineage_data->data->Count();
	size += lineage_data->data->Size();
	lineage.push_back(lineage_data);
	index.push_back(count);
}

shared_ptr<LineageDataWithOffset> LineageNested::GetInternal() {
	return lineage[ret_idx++];
}

bool LineageNested::IsComplete() {
	auto flag = ret_idx >= lineage.size();
	if (flag) {
		ret_idx = 0;
	}
	return flag;
}

int LineageNested::LocateChunkIndex(idx_t source) {
	// (1) locate which internal lineage_data to use
	auto lower = lower_bound(index.begin(), index.end(), source);
	if (lower == index.end()) {
		throw std::logic_error("Source not found in index");
	}
	// exact match, the value is in the next chunk
	if (*lower == source) {
		return lower-index.begin()+1;
	}

	return lower-index.begin();
}

shared_ptr<LineageDataWithOffset>& LineageNested::GetChunkAt(idx_t index) {
	return lineage[index];
}

idx_t LineageNested::GetAccCount(idx_t i) {
	return index[i];
}

} // namespace duckdb
#endif
