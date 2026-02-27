include(AddGoogleTest)

add_executable(datasource_test datasource_test.cpp)

target_link_libraries(datasource_test PRIVATE gtest GTest::gtest_main datasource)

include(GoogleTest)
gtest_discover_tests(datasource_test)
