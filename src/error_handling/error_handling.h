//
// Created by weronika on 12/28/24.
//

#ifndef ERROR_HANDLING_H
#define ERROR_HANDLING_H

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

typedef struct ErrorInfo {
    int error_code;
    char error_message[256];
    char inner_error_message[256];
} ErrorInfo;

#define LOG_ERR_AND_EXIT(source) (perror(source), fprintf(stderr, "%s:%d\n", __FILE__, __LINE__), exit(EXIT_FAILURE))

#define LOG_ERR(source) (perror(source), fprintf(stderr, "%s:%d\n", __FILE__, __LINE__))
#define LOG_THREAD_ERR(source, pid) (perror(source), fprintf(stderr, "[%d] %s:%d\n", pid, __FILE__, __LINE__))

#define LOG_INTERNAL_ERROR(reason) (fprintf(stderr, "%s: at %s:%d\n", reason, __FILE__, __LINE__))
#define LOG_INTERNAL_THREAD_ERROR(reason, pid) (fprintf(stderr, "[%d] %s: at %s:%d\n", pid, reason, __FILE__, __LINE__))

#endif //ERROR_HANDLING_H
