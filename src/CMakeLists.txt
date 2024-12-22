cmake_minimum_required(VERSION 3.28)
project(ExecutorApp C)

find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)

pkg_check_modules(PARQUET REQUIRED parquet-glib)

include_directories(${PARQUET_INCLUDE_DIRS} ${PROTOBUF_INCLUDE_DIRS})

add_executable(ExecutorApp main.c
        main_thread/main_thread.h
        main_thread/main_thread.c
        error_utilites/error_utilites.h
        socket_utilities/socket_utilities.h
        socket_utilities/socket_utilities.c
        consts/boolean.h
        socket_utilities/request_protocol/request_protocol.h
        socket_utilities/request_protocol/request_protocol.c
        proto_definitions/query_request.pb-c.c
        proto_definitions/aggregate.pb-c.c
        proto_definitions/aggregate.pb-c.h
        proto_definitions/query_request.pb-c.h
        worker_group/workers/worker.h
        worker_group/workers/worker.c
        hash_table/hash_table.h
        hash_table/hash_table.c
        proto_definitions/query_response.pb-c.h
        proto_definitions/query_response.pb-c.c
        temporary_testing/test.h
        temporary_testing/test.c
        worker_group/worker_group.h
        worker_group/worker_group.c
        worker_group/thread_data.h
        parquet_helpers/parquet_helpers.h
        parquet_helpers/parquet_helpers.c
        worker_group/aggregate_function.h
        converters/hash_table_to_query_response_converter.h
        converters/hash_table_to_query_response_converter.c
        proto_definitions/query_result.pb-c.c
        proto_definitions/query_result.pb-c.h
        client_array/client_array.h
        client_array/client_array.c
        executors_server/executors_server.h
        executors_server/executors_server.c
)
include_directories(
        error_utilites
        consts
        main_thread
        socket_utilities
        hash_table
        proto_definitions
        worker_group
        converters
        client_array
        executors_server
)

target_link_libraries(ExecutorApp ${PARQUET_LIBRARIES})
target_link_libraries(ExecutorApp protobuf-c)

# memory sanitizer
#if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
#    # Enable AddressSanitizer for both the compiler and linker
#    target_compile_options(ExecutorApp PRIVATE -fsanitize=address -g)
#    target_link_options(ExecutorApp PRIVATE -fsanitize=address)
#endif()

