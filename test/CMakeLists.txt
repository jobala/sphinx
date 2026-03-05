include(AddGoogleTest)

add_executable(datasource_test datasource_test.cpp)
add_executable(logical_plan_test logical_plan_test.cpp)

target_link_libraries(datasource_test PRIVATE gtest GTest::gtest_main datasource)
target_link_libraries(logical_plan_test PRIVATE gtest GTest::gtest_main logical_plan datasource)

include(GoogleTest)
gtest_discover_tests(datasource_test)
gtest_discover_tests(logical_plan_test)
