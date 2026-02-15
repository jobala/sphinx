add_library(datasource  csv_datasource_iter.cpp csv_datasource.cpp)
target_include_directories(datasource PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(datasource PUBLIC types)
