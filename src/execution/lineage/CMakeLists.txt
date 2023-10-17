add_library_unity(duckdb_lineage OBJECT lineage_manager.cpp lineage_persist.cpp operator_lineage.cpp
  lineage_stats.cpp lineage_index.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_lineage>
    PARENT_SCOPE)
