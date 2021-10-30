#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/sorted_block.hpp"

#include <numeric>

namespace duckdb {

static idx_t GetSortingColSize(const LogicalType &type) {
	auto physical_type = type.InternalType();
	if (TypeIsConstantSize(physical_type)) {
		return GetTypeIdSize(physical_type);
	} else {
		switch (physical_type) {
		case PhysicalType::VARCHAR:
			// TODO: make use of statistics
			return string_t::INLINE_LENGTH;
		case PhysicalType::LIST:
			// Lists get another byte to denote the empty list
			return 2 + GetSortingColSize(ListType::GetChildType(type));
		case PhysicalType::MAP:
		case PhysicalType::STRUCT:
			return 1 + GetSortingColSize(StructType::GetChildType(type, 0));
		default:
			throw NotImplementedException("Unable to order column with type %s", type.ToString());
		}
	}
}

SortLayout::SortLayout(const vector<BoundOrderByNode> &orders)
    : column_count(orders.size()), all_constant(true), comparison_size(0), entry_size(0) {
	vector<LogicalType> blob_layout_types;
	for (idx_t i = 0; i < orders.size(); i++) {
		const auto &order = orders[i];

		order_types.push_back(order.type);
		order_by_null_types.push_back(order.null_order);
		auto &expr = *order.expression;
		logical_types.push_back(expr.return_type);

		auto physical_type = expr.return_type.InternalType();
		all_constant = all_constant && TypeIsConstantSize(physical_type);
		constant_size.push_back(TypeIsConstantSize(physical_type));
		column_sizes.push_back(0);
		auto &col_size = column_sizes.back();

		if (order.stats) {
			stats.push_back(order.stats.get());
			has_null.push_back(stats.back()->CanHaveNull());
		} else {
			stats.push_back(nullptr);
			has_null.push_back(true);
		}

		col_size += has_null.back() ? 1 : 0;
		if (TypeIsConstantSize(physical_type)) {
			col_size += GetTypeIdSize(physical_type);
		} else {
			col_size += GetSortingColSize(expr.return_type);
			sorting_to_blob_col[i] = blob_layout_types.size();
			blob_layout_types.push_back(expr.return_type);
		}
		comparison_size += col_size;
	}
	entry_size = comparison_size + sizeof(idx_t);
	blob_layout.Initialize(blob_layout_types, false);
}

LocalSortState::LocalSortState() : initialized(false) {
}

static idx_t EntriesPerBlock(idx_t width) {
	return (Storage::BLOCK_SIZE + width * STANDARD_VECTOR_SIZE - 1) / width;
}

void LocalSortState::Initialize(GlobalSortState &global_sort_state, BufferManager &buffer_manager_p) {
	sort_layout = &global_sort_state.sort_layout;
	payload_layout = &global_sort_state.payload_layout;
	buffer_manager = &buffer_manager_p;
	// Radix sorting data
	radix_sorting_data = make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(sort_layout->entry_size),
	                                                    sort_layout->entry_size);
	// Blob sorting data
	if (!sort_layout->all_constant) {
		auto blob_row_width = sort_layout->blob_layout.GetRowWidth();
		blob_sorting_data =
		    make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(blob_row_width), blob_row_width);
		blob_sorting_heap = make_unique<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	}
	// Payload data
	auto payload_row_width = payload_layout->GetRowWidth();
	payload_data =
	    make_unique<RowDataCollection>(*buffer_manager, EntriesPerBlock(payload_row_width), payload_row_width);
	payload_heap = make_unique<RowDataCollection>(*buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
	// Init done
	initialized = true;
}

void LocalSortState::SinkChunk(DataChunk &sort, DataChunk &payload) {
	D_ASSERT(sort.size() == payload.size());
	// Build and serialize sorting data to radix sortable rows
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);
	auto handles = radix_sorting_data->Build(sort.size(), data_pointers, nullptr);
	for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
		bool has_null = sort_layout->has_null[sort_col];
		bool nulls_first = sort_layout->order_by_null_types[sort_col] == OrderByNullType::NULLS_FIRST;
		bool desc = sort_layout->order_types[sort_col] == OrderType::DESCENDING;
		// TODO: use actual string statistics
		RowOperations::RadixScatter(sort.data[sort_col], sort.size(), sel_ptr, sort.size(), data_pointers, desc,
		                            has_null, nulls_first, string_t::INLINE_LENGTH,
		                            sort_layout->column_sizes[sort_col]);
	}

	// Also fully serialize blob sorting columns (to be able to break ties
	if (!sort_layout->all_constant) {
		DataChunk blob_chunk;
		blob_chunk.SetCardinality(sort.size());
		for (idx_t sort_col = 0; sort_col < sort.ColumnCount(); sort_col++) {
			if (!TypeIsConstantSize(sort.data[sort_col].GetType().InternalType())) {
				blob_chunk.data.emplace_back(sort.data[sort_col]);
			}
		}
		handles = blob_sorting_data->Build(blob_chunk.size(), data_pointers, nullptr);
		auto blob_data = blob_chunk.Orrify();
		RowOperations::Scatter(blob_chunk, blob_data.get(), sort_layout->blob_layout, addresses, *blob_sorting_heap,
		                       sel_ptr, blob_chunk.size());
	}

	// Finally, serialize payload data
	handles = payload_data->Build(payload.size(), data_pointers, nullptr);
	auto input_data = payload.Orrify();
	RowOperations::Scatter(payload, input_data.get(), *payload_layout, addresses, *payload_heap, sel_ptr,
	                       payload.size());
}

idx_t LocalSortState::SizeInBytes() const {
	idx_t size_in_bytes = radix_sorting_data->SizeInBytes() + payload_data->SizeInBytes();
	if (!sort_layout->all_constant) {
		size_in_bytes += blob_sorting_data->SizeInBytes() + blob_sorting_heap->SizeInBytes();
	}
	if (!payload_layout->AllConstant()) {
		size_in_bytes += payload_heap->SizeInBytes();
	}
	return size_in_bytes;
}

void LocalSortState::Sort(GlobalSortState &global_sort_state) {
	D_ASSERT(radix_sorting_data->count == payload_data->count);
	if (radix_sorting_data->count == 0) {
		return;
	}
#ifdef LINEAGE
	idx_t count = radix_sorting_data->count;
#endif
	// Move all data to a single SortedBlock
	sorted_blocks.emplace_back(make_unique<SortedBlock>(*buffer_manager, global_sort_state));
	auto &sb = *sorted_blocks.back();
	// Fixed-size sorting data
	auto sorting_block = ConcatenateBlocks(*radix_sorting_data);
	sb.radix_sorting_data.push_back(move(sorting_block));
	// Variable-size sorting data
	if (!sort_layout->all_constant) {
		auto &blob_data = *blob_sorting_data;
		auto new_block = ConcatenateBlocks(blob_data);
		sb.blob_sorting_data->data_blocks.push_back(move(new_block));
	}
	// Payload data
	auto payload_block = ConcatenateBlocks(*payload_data);
	sb.payload_data->data_blocks.push_back(move(payload_block));
	// Now perform the actual sort
	SortInMemory();
#ifdef LINEAGE
	SelectionVector lineage_sel = SelectionVector(count);
	// Re-order before the merge sort
	ReOrder(global_sort_state, &lineage_sel);
	lineage = make_shared<LineageSelVec>(lineage_sel, count);
#else
	// Re-order before the merge sort
	ReOrder(global_sort_state);
#endif
}

RowDataBlock LocalSortState::ConcatenateBlocks(RowDataCollection &row_data) {
	// Create block with the correct capacity
	const idx_t &entry_size = row_data.entry_size;
	idx_t capacity = MaxValue(((idx_t)Storage::BLOCK_SIZE + entry_size - 1) / entry_size, row_data.count);
	RowDataBlock new_block(*buffer_manager, capacity, entry_size);
	new_block.count = row_data.count;
	auto new_block_handle = buffer_manager->Pin(new_block.block);
	data_ptr_t new_block_ptr = new_block_handle->Ptr();
	// Copy the data of the blocks into a single block
	for (auto &block : row_data.blocks) {
		auto block_handle = buffer_manager->Pin(block.block);
		memcpy(new_block_ptr, block_handle->Ptr(), block.count * entry_size);
		new_block_ptr += block.count * entry_size;
	}
	row_data.blocks.clear();
	row_data.count = 0;
	return new_block;
}

#ifdef LINEAGE
void LocalSortState::ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate,
                             SelectionVector *lineage_sel) {
#else
void LocalSortState::ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate) {
#endif
	auto &unordered_data_block = sd.data_blocks.back();
	const idx_t &count = unordered_data_block.count;
	auto unordered_data_handle = buffer_manager->Pin(unordered_data_block.block);
	const data_ptr_t unordered_data_ptr = unordered_data_handle->Ptr();
	// Create new block that will hold re-ordered row data
	RowDataBlock ordered_data_block(*buffer_manager, unordered_data_block.capacity, unordered_data_block.entry_size);
	ordered_data_block.count = count;
	auto ordered_data_handle = buffer_manager->Pin(ordered_data_block.block);
	data_ptr_t ordered_data_ptr = ordered_data_handle->Ptr();
	// Re-order fixed-size row layout
	const idx_t row_width = sd.layout.GetRowWidth();
	const idx_t sorting_entry_size = gstate.sort_layout.entry_size;
	for (idx_t i = 0; i < count; i++) {
		idx_t index = Load<idx_t>(sorting_ptr);
#ifdef LINEAGE
		lineage_sel->set_index(i, index);
#endif
		memcpy(ordered_data_ptr, unordered_data_ptr + index * row_width, row_width);
		ordered_data_ptr += row_width;
		sorting_ptr += sorting_entry_size;
	}
	// Replace the unordered data block with the re-ordered data block
	sd.data_blocks.clear();
	sd.data_blocks.push_back(move(ordered_data_block));
	// Deal with the heap (if necessary)
	if (!sd.layout.AllConstant()) {
		// Swizzle the column pointers to offsets
		RowOperations::SwizzleColumns(sd.layout, ordered_data_handle->Ptr(), count);
		// Create a single heap block to store the ordered heap
		idx_t total_byte_offset = std::accumulate(heap.blocks.begin(), heap.blocks.end(), 0,
		                                          [](idx_t a, const RowDataBlock &b) { return a + b.byte_offset; });
		idx_t heap_block_size = MaxValue(total_byte_offset, (idx_t)Storage::BLOCK_SIZE);
		RowDataBlock ordered_heap_block(*buffer_manager, heap_block_size, 1);
		ordered_heap_block.count = count;
		ordered_heap_block.byte_offset = total_byte_offset;
		auto ordered_heap_handle = buffer_manager->Pin(ordered_heap_block.block);
		data_ptr_t ordered_heap_ptr = ordered_heap_handle->Ptr();
		// Fill the heap in order
		ordered_data_ptr = ordered_data_handle->Ptr();
		const idx_t heap_pointer_offset = sd.layout.GetHeapPointerOffset();
		for (idx_t i = 0; i < count; i++) {
			auto heap_row_ptr = Load<data_ptr_t>(ordered_data_ptr + heap_pointer_offset);
			auto heap_row_size = Load<idx_t>(heap_row_ptr);
			memcpy(ordered_heap_ptr, heap_row_ptr, heap_row_size);
			ordered_heap_ptr += heap_row_size;
			ordered_data_ptr += row_width;
		}
		// Swizzle the base pointer to the offset of each row in the heap
		RowOperations::SwizzleHeapPointer(sd.layout, ordered_data_handle->Ptr(), ordered_heap_handle->Ptr(), count);
		// Move the re-ordered heap to the SortedData, and clear the local heap
		sd.heap_blocks.push_back(move(ordered_heap_block));
		heap.pinned_blocks.clear();
		heap.blocks.clear();
		heap.count = 0;
	}
}

#ifdef LINEAGE
void LocalSortState::ReOrder(GlobalSortState &gstate, SelectionVector *lineage_sel) {
#else
void LocalSortState::ReOrder(GlobalSortState &gstate) {
#endif
	auto &sb = *sorted_blocks.back();
	auto sorting_handle = buffer_manager->Pin(sb.radix_sorting_data.back().block);
	const data_ptr_t sorting_ptr = sorting_handle->Ptr() + gstate.sort_layout.comparison_size;
	// Re-order variable size sorting columns
	if (!gstate.sort_layout.all_constant) {
#ifdef LINEAGE
		ReOrder(*sb.blob_sorting_data, sorting_ptr, *blob_sorting_heap, gstate, lineage_sel);
#else
		ReOrder(*sb.blob_sorting_data, sorting_ptr, *blob_sorting_heap, gstate);
#endif
	}
	// And the payload
#ifdef LINEAGE
	ReOrder(*sb.payload_data, sorting_ptr, *payload_heap, gstate, lineage_sel);
#else
	ReOrder(*sb.payload_data, sorting_ptr, *payload_heap, gstate);
#endif
}

GlobalSortState::GlobalSortState(BufferManager &buffer_manager, const vector<BoundOrderByNode> &orders,
                                 RowLayout &payload_layout)
    : buffer_manager(buffer_manager), sort_layout(SortLayout(orders)), payload_layout(payload_layout),
      block_capacity(0), external(false) {
}

void GlobalSortState::AddLocalState(LocalSortState &local_sort_state) {
	if (!local_sort_state.radix_sorting_data) {
		return;
	}

	// Sort accumulated data
	local_sort_state.Sort(*this);

	// Append local state sorted data to this global state
	lock_guard<mutex> append_guard(lock);
	for (auto &sb : local_sort_state.sorted_blocks) {
		sorted_blocks.push_back(move(sb));
	}
}

void GlobalSortState::PrepareMergePhase() {
	// Determine if we need to use do an external sort
	idx_t total_heap_size =
	    std::accumulate(sorted_blocks.begin(), sorted_blocks.end(), (idx_t)0,
	                    [](idx_t a, const unique_ptr<SortedBlock> &b) { return a + b->HeapSize(); });
	if (external || total_heap_size > 0.25 * buffer_manager.GetMaxMemory()) {
		external = true;
	}
	// Use the data that we have to determine which partition size to use during the merge
	if (total_heap_size > 0) {
		// If we have variable size data we need to be conservative, as there might be skew
		idx_t max_block_size = 0;
		for (auto &sb : sorted_blocks) {
			idx_t size_in_bytes = sb->SizeInBytes();
			if (size_in_bytes > max_block_size) {
				max_block_size = size_in_bytes;
				block_capacity = sb->Count();
			}
		}
	} else {
		for (auto &sb : sorted_blocks) {
			block_capacity = MaxValue(block_capacity, sb->Count());
		}
	}
	// Unswizzle and pin heap blocks if we can fit everything in memory
	if (!external) {
		for (auto &sb : sorted_blocks) {
			sb->blob_sorting_data->Unswizzle();
			sb->payload_data->Unswizzle();
		}
	}
}

void GlobalSortState::InitializeMergeRound() {
	D_ASSERT(sorted_blocks_temp.empty());
	// If we reverse this list, the blocks that were merged last will be merged first in the next round
	// These are still in memory, therefore this reduces the amount of read/write to disk!
	std::reverse(sorted_blocks.begin(), sorted_blocks.end());
	// Uneven number of blocks - keep one on the side
	if (sorted_blocks.size() % 2 == 1) {
		odd_one_out = move(sorted_blocks.back());
		sorted_blocks.pop_back();
	}
	// Init merge path path indices
	pair_idx = 0;
	num_pairs = sorted_blocks.size() / 2;
	l_start = 0;
	r_start = 0;
	// Allocate room for merge results
	for (idx_t p_idx = 0; p_idx < num_pairs; p_idx++) {
		sorted_blocks_temp.emplace_back();
	}
}

void GlobalSortState::CompleteMergeRound() {
	sorted_blocks.clear();
	for (auto &sorted_block_vector : sorted_blocks_temp) {
		sorted_blocks.push_back(make_unique<SortedBlock>(buffer_manager, *this));
		sorted_blocks.back()->AppendSortedBlocks(sorted_block_vector);
	}
	sorted_blocks_temp.clear();
	if (odd_one_out) {
		sorted_blocks.push_back(move(odd_one_out));
		odd_one_out = nullptr;
	}
	// Only one block left: Done!
	if (sorted_blocks.size() == 1) {
		sorted_blocks[0]->radix_sorting_data.clear();
		sorted_blocks[0]->blob_sorting_data = nullptr;
	}
}

} // namespace duckdb
