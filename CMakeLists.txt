cmake_minimum_required(VERSION 3.29)
project(ExecutorApp C)

set(CMAKE_C_STANDARD 11)

add_executable(ExecutorApp main.c
        main_thread/main_thread.h
        main_thread/main_thread.c
        error_utilites/error_utilites.h
        socket_utilities/socket_utilities.h
        socket_utilities/socket_utilities.c
        consts/boolean.h
)
include_directories(
        error_utilites
        consts
        main_thread
        socket_utilities
)
