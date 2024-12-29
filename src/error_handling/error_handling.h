//
// Created by weronika on 12/28/24.
//

#ifndef ERROR_HANDLING_H
#define ERROR_HANDLING_H

#include <stdio.h>
#include <stdlib.h>

typedef struct ErrorInfo {
    int error_code;
    char error_message[256];
    char inner_error_message[256];
} ErrorInfo;

#define NO_ERROR 0
#define INTERNAL_ERROR -1
#define G_ERROR -2

#define SET_ERR(err, code, msg, inner_msg) do { \
    if (err) { \
        (err)->error_code = (code); \
        snprintf((err)->error_message, sizeof((err)->error_message), "%s", (msg)); \
        snprintf((err)->inner_error_message, sizeof((err)->inner_error_message), "%s", (inner_msg)); \
    } \
} while (0)

#define CLEAR_ERR(err) do { \
    if (err) { \
        (err)->error_code = NO_ERROR; \
        (err)->error_message[0] = '\0'; \
        (err)->inner_error_message[0] = '\0'; \
    } \
} while (0)

#define LOG_ERR_AND_EXIT(source) (perror(source), fprintf(stderr, "%s:%d\n", __FILE__, __LINE__), exit(EXIT_FAILURE))

#define LOG_ERR(source) (perror(source), fprintf(stderr, "%s:%d\n", __FILE__, __LINE__))
#define LOG_THREAD_ERR(source, pid) (perror(source), fprintf(stderr, "[%d] %s:%d\n", pid, __FILE__, __LINE__))

#define LOG_INTERNAL_ERR(reason) (fprintf(stderr, "%s: at %s:%d\n", reason, __FILE__, __LINE__))
#define LOG_INTERNAL_THREAD_ERR(reason, pid) (fprintf(stderr, "[%d] %s: at %s:%d\n", pid, reason, __FILE__, __LINE__))

#endif //ERROR_HANDLING_H
