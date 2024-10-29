//
// Created by karol on 29.10.24.
//

#ifndef VALUE_H
#define VALUE_H

#include "results.h"

struct value {
    char* grouping_value;
    Results results;
};

typedef struct value Value;

char* to_string(Value value);

#endif //VALUE_H
