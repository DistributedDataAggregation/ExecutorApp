cmake_minimum_required(VERSION 3.29)
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
        socket_utilities/proto_definitions/query_request.pb-c.c
        socket_utilities/proto_definitions/aggregate.pb-c.c
        socket_utilities/proto_definitions/aggregate.pb-c.h
        socket_utilities/proto_definitions/query_request.pb-c.h
        socket_utilities/proto_definitions/result.pb-c.c
        socket_utilities/proto_definitions/result.pb-c.h
)
include_directories(
        error_utilites
        consts
        main_thread
        socket_utilities
        hash_table
)

target_link_libraries(ExecutorApp ${PARQUET_LIBRARIES})
target_link_libraries(ExecutorApp protobuf-c)

