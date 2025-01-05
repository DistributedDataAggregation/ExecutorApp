set(TEST_FILES
        tests_converters.cc
        tests_hash_map.cc
        tests_worker_group.cc
        tests_worker.cc
        tests_hash_map_optimized.cc
)
enable_testing()

foreach (TEST_FILE IN LISTS TEST_FILES)
    string(REPLACE ".cc" "" TEST_NAME ${TEST_FILE})
    add_executable(${TEST_NAME} ${TEST_FILE})
    target_include_directories(${TEST_NAME} PUBLIC ${CMAKE_SOURCE_DIR}/include)
    target_link_libraries(${TEST_NAME} PUBLIC GTest::gtest GTest::gtest_main ExecutorLib)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
    gtest_discover_tests(${TEST_NAME})
endforeach ()

