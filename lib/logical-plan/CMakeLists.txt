add_library(logical_plan scan.cpp expr/column.cpp)
target_include_directories(logical_plan PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(logical_plan PUBLIC types datasource)
