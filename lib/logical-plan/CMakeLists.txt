add_library(logical_plan scan.cpp expr/column.cpp projection.cpp)
target_include_directories(logical_plan PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/expr)
target_link_libraries(logical_plan PUBLIC types datasource)
