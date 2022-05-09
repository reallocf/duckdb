#ifdef LINEAGE
#include "duckdb/execution/lineage/lineage_data.hpp"
#include "duckdb/parser/statement/create_statement.hpp"

namespace duckdb {
// LineageDataRowVector

idx_t LineageDataRowVector::Count() {
	return count;
}

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

idx_t LineageDataRowVector::Size() {
	return count * sizeof(vec[0]);
}

idx_t LineageDataRowVector::Backward(idx_t source) {
	return (idx_t)vec[source];
}

void LineageDataRowVector::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageDataRowVector::GetChild() {
	return child;
}


// LineageDataVectorBufferArray

idx_t LineageDataVectorBufferArray::Count() {
	return count;
}

void LineageDataVectorBufferArray::Debug() {
	std::cout << "LineageDataVectorBufferArray " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataVectorBufferArray::Process(idx_t offset) {
	//if (offset == 0) return (data_ptr_t)vec.get();
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.get();
}

idx_t LineageDataVectorBufferArray::Size() {
  if (count)
    // sizeof vector is always STANDARD_VECTOR_SIZE since that is how
    // much memory allocated
    // return count * sizeof(vec[0]);
    return STANDARD_VECTOR_SIZE * sizeof(vec[0]);
  else
    return 0;
}

idx_t LineageDataVectorBufferArray::Backward(idx_t source) {
	return (idx_t)vec[source];
}

void LineageDataVectorBufferArray::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageDataVectorBufferArray::GetChild() {
	return child;
}


// LineageDataUIntPtrArray

idx_t LineageDataUIntPtrArray::Count() {
	return count;
}

void LineageDataUIntPtrArray::Debug() {
	std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataUIntPtrArray::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.get();
}

idx_t LineageDataUIntPtrArray::Size() {
  if (count)
    // sizeof vector is always STANDARD_VECTOR_SIZE since that is how
    // much memory allocated
    // return count * sizeof(vec[0]);
    return STANDARD_VECTOR_SIZE * sizeof(vec[0]);
  else
    return 0;
}

idx_t LineageDataUIntPtrArray::Backward(idx_t source) {
	return (idx_t)vec[source];
}

void LineageDataUIntPtrArray::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageDataUIntPtrArray::GetChild() {
	return child;
}


// LineageDataUInt32Array

idx_t LineageDataUInt32Array::Count() {
	return count;
}

void LineageDataUInt32Array::Debug() {
	std::cout << "LineageDataArray " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec[i] << ") ";
	}
	std::cout << std::endl;
}

data_ptr_t LineageDataUInt32Array::Process(idx_t offset) {
	for (idx_t i = 0; i < count; i++) {
		vec[i] += offset;
	}
	return (data_ptr_t)vec.get();
}

idx_t LineageDataUInt32Array::Size() {
  if (count)
    // sizeof vector is always STANDARD_VECTOR_SIZE since that is how
    // much memory allocated
    // return count  * sizeof(vec[0]);
    return STANDARD_VECTOR_SIZE * sizeof(vec[0]);
  else
    return 0;
}

idx_t LineageDataUInt32Array::Backward(idx_t source) {
	return (idx_t)vec[source];
}

void LineageDataUInt32Array::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageDataUInt32Array::GetChild() {
	return child;
}


// LineageSelVec

idx_t LineageSelVec::Count() {
	return count;
}

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

idx_t LineageSelVec::Size() {
	return count * sizeof(vec.get_index(0));
  if (count)
    // sizeof vector is always STANDARD_VECTOR_SIZE since that is how
    // much memory allocated
    // return count * sizeof(vec.get_index(0));
    return STANDARD_VECTOR_SIZE * sizeof(vec.get_index(0));
  else
    return 0;
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

void LineageSelVec::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageSelVec::GetChild() {
	return child;
}


// LineageRange

idx_t LineageRange::Count() {
	return end - start;
}

void LineageRange::Debug() {
	std::cout << "LineageRange - Start: " << start << " End: " << end << std::endl;
}

data_ptr_t LineageRange::Process(idx_t offset) {
	// Lazily convert lineage range to selection vector
	if (vec.empty()) {
		for (idx_t i = start; i < end; i++) {
			vec.push_back(i + offset);
		}
	}
	return (data_ptr_t)vec.data();
}

idx_t LineageRange::Size() {
  // in-memory count=2, persist count=end-start
  // return 2 * sizeof(sel_t);
  return (end-start) * sizeof(sel_t);
}

idx_t LineageRange::Backward(idx_t source) {
	return source;
}

void LineageRange::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageRange::GetChild() {
	return child;
}


// LineageConstant

idx_t LineageConstant::Count() {
	return count;
}

void LineageConstant::Debug() {
	std::cout << "LineageConstant - value: " << value << " Count: " << count << std::endl;
}

data_ptr_t LineageConstant::Process(idx_t offset) {
	vec.push_back(value);
	return (data_ptr_t)vec.data();
}

idx_t LineageConstant::Size() {
	return 1*sizeof(value);
}

idx_t LineageConstant::Backward(idx_t source) {
	return value;
}

void LineageConstant::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageConstant::GetChild() {
	return child;
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

idx_t LineageBinary::Backward(idx_t source) {
	throw std::logic_error("Can't call backward directly on LineageBinary");
}

void LineageBinary::SetChild(shared_ptr<LineageDataWithOffset> c) {
	child = c;
}

shared_ptr<LineageDataWithOffset> LineageBinary::GetChild() {
	return child;
}


// LineageNested

idx_t LineageNested::Count() {
	return count;
}

void LineageNested::Debug() {
	std::cout << "LineageNested:" << std::endl;
	for (const shared_ptr<LineageDataWithOffset>& lineage_data : lineage) {
		std::cout << "    ";
		lineage_data->data->Debug();
	}
	std::cout << "End LineageNested" << std::endl;
}

data_ptr_t LineageNested::Process(idx_t offset) {
	throw std::logic_error("Can't call process on LineageNested");
}

idx_t LineageNested::Size() {
	return size;
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

idx_t LineageNested::Backward(idx_t source) {
	throw std::logic_error("Can't call backward directly on LineageNested");
}

void LineageNested::SetChild(shared_ptr<LineageDataWithOffset> c) {
	// Do nothing since the children are set on the internal LineageData
}

shared_ptr<LineageDataWithOffset> LineageNested::GetChild() {
	throw std::logic_error("Can't call GetChild on LineageNested");
}

} // namespace duckdb
#endif
