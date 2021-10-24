add_library_unity(duckdb_lineage OBJECT lineage.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_lineage>
    PARENT_SCOPE)
