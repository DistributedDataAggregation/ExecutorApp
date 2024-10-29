//
// Created by karol on 29.10.24.
//

#ifndef RESULTS_H
#define RESULTS_H

#include "request_protocol/operation.h"
#include "result_type.h"

struct results {
    int count;
    ResultType* values;
    Operation* operations;
};

typedef struct results Results;

#endif //RESULTS_H
