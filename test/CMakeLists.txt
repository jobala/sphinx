include(AddGoogleTest)

add_executable(unit_tests test_demo.cpp)
add_executable(datasource_test datasource_test.cpp)

target_link_libraries(unit_tests PRIVATE  gtest GTest::gtest_main)
target_link_libraries(datasource_test PRIVATE gtest GTest::gtest_main datasource)

include(GoogleTest)
gtest_discover_tests(unit_tests)
gtest_discover_tests(datasource_test)
