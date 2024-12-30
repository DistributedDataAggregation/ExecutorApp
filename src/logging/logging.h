
#ifndef LOGGING_H
#define LOGGING_H

#ifdef DEBUG_BUILD
#include <stdio.h>
#define LOG(fmt, ...) printf("DEBUG: " fmt "\n", ##__VA_ARGS__)
#else
#define LOG(fmt, ...)
#endif

#endif //LOGGING_H