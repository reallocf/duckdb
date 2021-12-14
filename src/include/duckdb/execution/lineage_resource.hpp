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

	    LineageResource(){
		    chunks = make_shared<ChunkCollection>();
	    }

	    static std::shared_ptr<ChunkCollection> chunks;

	    void assemble(DataChunk &chunk){
		    chunks->Append(chunk);
	    }

	    shared_ptr<ChunkCollection> disperseAll(){
		    return chunks;
	    }
};

}
