#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {

// LineageDataRowVector
void LineageDataRowVector::Debug() {
	std::cout << "LineageDataVector " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataRowVector::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.data();
}

// LineageSelVec

void LineageSelVec::Debug() {
	std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] + in_offset << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageSelVec::Process(idx_t offset) {
	if (processed == false) {
		for (idx_t i = 0; i < count; i++) {
			*(vec.data() + i) += offset + in_offset;
		}
		processed = true;
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageSelVec::At(idx_t source) {
	D_ASSERT(source < count);
	return (idx_t)vec.sel_data()->owned_data[source] + in_offset;
}

vector<LineageDataWithOffset> LineageSelVec::Divide(idx_t child_offset) {
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
		res[i] = {this_lineage, (int)child_offset, this_offset}; // I don't think this offset management is correct, but we don't use this anymore so it's fine
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

void LineageVec::Debug() {
	std::cout << "LineageVec:" << std::endl;
  for (idx_t j = 0; j < lineage_vec->size(); ++j) {
    (*lineage_vec)[j]->Debug();
  }
	std::cout << "End LineageVec" << std::endl;
}

idx_t LineageVec::BuildInnerIndex() {
  if (index.size() > 0) return count;
  idx_t inner_count = 0;
  for (idx_t j = 0; j < lineage_vec->size(); ++j) {
    inner_count += (*lineage_vec)[j]->Count();
	  index.push_back(inner_count);
  }
  count = inner_count;
  return inner_count;
}

LineageDataWithOffset LineageVec::GetInternal() {
  auto data = (*lineage_vec)[ret_idx++];
  return LineageDataWithOffset{ data, (int)data->child_offset, 0 };
}


bool LineageVec::IsComplete() {
	auto flag = ret_idx >= lineage_vec->size();
	if (flag) {
		ret_idx = 0;
	}
	return flag;
}

int LineageVec::LocateChunkIndex(idx_t source) {
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

LineageDataWithOffset LineageVec::GetChunkAt(idx_t index) {
  auto data = (*lineage_vec)[index++];
  return LineageDataWithOffset{ data, (int)data->child_offset, 0 };
}

idx_t LineageVec::GetAccCount(idx_t i) {
	return index[i];
}

} // namespace duckdb
#endif
