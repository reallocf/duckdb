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
	if (offset == 0) return (data_ptr_t)vec.get();
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


// LineageSelVec

idx_t LineageSelVec::Count() {
	return count;
}

void LineageSelVec::Debug() {
	std::cout << "LineageSelVec " << " " << typeid(vec).name() << std::endl;
	for (idx_t i = 0; i < count; i++) {
		std::cout << " (" << i << " -> " << vec.sel_data()->owned_data[i] << ") ";
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
		res[i] = {make_shared<LineageSelVec>(SelectionVector(this_data), this_count), (int)this_offset};
	}
	return res;
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


} // namespace duckdb
#endif
