//
// Created by karol on 27.10.24.
//

#ifndef AGGREGATION_H
#define AGGREGATION_H

#include "operation.h"

struct aggregation {
    char* column;
    Operation operation;
};
typedef struct aggregation Aggregation;

#endif //AGGREGATION_H
