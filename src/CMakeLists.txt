cmake_minimum_required(VERSION 3.28)
project(ExecutorApp C CXX)

enable_testing()

option(ENABLE_UNIT_TESTING "Enable unit tests" ON)

find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)

pkg_check_modules(PARQUET REQUIRED parquet-glib)

add_library(ExecutorLib STATIC
        main_thread/main_thread.c
        socket_utilities/socket_utilities.c
        socket_utilities/request_protocol/request_protocol.c
        proto_definitions/query_request.pb-c.c
        proto_definitions/aggregate.pb-c.c
        proto_definitions/query_response.pb-c.c
        proto_definitions/query_result.pb-c.c
        hash_table/hash_table.c
        worker_group/worker_group.c
        worker_group/workers/worker.c
        parquet_helpers/parquet_helpers.c
        client_array/client_array.c
        executors_server/executors_server.c
        converters/hash_table_to_query_response_converter.c
        error_handling/error_handling.h
        logging/logging.h
)

target_include_directories(ExecutorLib PUBLIC
        ${PARQUET_INCLUDE_DIRS}
        ${PROTOBUF_INCLUDE_DIRS}
        consts
        main_thread
        socket_utilities
        hash_table
        proto_definitions
        worker_group
        worker_group/workers
        converters
        client_array
        executors_server
        error_handling
        logging
)

target_link_libraries(ExecutorLib ${PARQUET_LIBRARIES} protobuf-c)

add_executable(ExecutorApp main.c)

target_link_libraries(ExecutorApp ExecutorLib)

if(ENABLE_UNIT_TESTING)
    include(CTest)

    include(FetchContent)
    FetchContent_Declare(
            googletest
            GIT_REPOSITORY https://github.com/google/googletest.git
            GIT_TAG v1.15.2
            GIT_SHALLOW TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    include(GoogleTest)
    add_subdirectory(unit_tests)
endif()
