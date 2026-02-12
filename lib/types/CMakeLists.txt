add_library(types types.cpp)
target_include_directories(types PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(types PUBLIC Arrow::arrow_shared)
