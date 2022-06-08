add_subdirectory(art)
add_library_unity(
  duckdb_index
  OBJECT
  lineage_index.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_index>
    PARENT_SCOPE)
