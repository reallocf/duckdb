//
// Created by w4118 on 12/7/21.
//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"



namespace duckdb{

class LineageResource{
	public:
		const int MAX_CHUNK_SIZE_THRESHOLD = 8;
	    const int MAX_DATA_CHUNK_THRESHOLD = 16;

	    vector<DataChunk> chunks;

	    void assemble(DataChunk chunk){
		    chunks.push_back(chunk);
	    }

	    vector<DataChunk> disperseAll(){
		    return chunks;
	    }
};

}
