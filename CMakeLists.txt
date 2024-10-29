cmake_minimum_required(VERSION 3.29)
project(ExecutorApp C)

set(CMAKE_C_STANDARD 11)

find_package(PkgConfig REQUIRED)
pkg_check_modules(PARQUET REQUIRED parquet-glib)

include_directories(${PARQUET_INCLUDE_DIRS})

add_executable(ExecutorApp main.c
        main_thread/main_thread.h
        main_thread/main_thread.c
        error_utilites/error_utilites.h
        socket_utilities/socket_utilities.h
        socket_utilities/socket_utilities.c
        consts/boolean.h
        socket_utilities/request_protocol/request_protocol.h
        socket_utilities/request_protocol/request.h
        socket_utilities/request_protocol/aggregation.h
        socket_utilities/request_protocol/operation.h
        socket_utilities/request_protocol/request_protocol.c
        hash_table/value.h
        hash_table/results.h
        hash_table/result_type.h
        hash_table/value.c
)
include_directories(
        error_utilites
        consts
        main_thread
        socket_utilities
        hash_table
)

target_link_libraries(ExecutorApp ${PARQUET_LIBRARIES})
