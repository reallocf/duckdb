//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_top.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
struct LineageDataWithOffset;

class LineageData {
public:
	virtual idx_t Count() = 0;
	virtual void Debug() = 0;
	virtual data_ptr_t Process(idx_t offset) = 0;
	virtual void SetChild(shared_ptr<LineageDataWithOffset> c) = 0;
	virtual shared_ptr<LineageDataWithOffset> GetChild() = 0;
	virtual idx_t Size() = 0;
	virtual idx_t Backward(idx_t) = 0;
	virtual ~LineageData() {};
};

struct LineageDataWithOffset {
	// TODO does this need to have a shared_ptr wrapper?
	shared_ptr<LineageData> data;
	idx_t this_offset;
	int child_offset;
};

struct SourceAndMaybeData {
	idx_t source;
	shared_ptr<LineageDataWithOffset> data;
};

} // namespace duckdb
#endif