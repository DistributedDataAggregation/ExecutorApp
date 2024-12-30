//
// Created by karol on 10.11.24.
//

#ifndef AGGREGATE_FUNCTION_H
#define AGGREGATE_FUNCTION_H

typedef enum aggregate_function {
    UNKNOWN = -1,
    MIN = 0,
    MAX = 1,
    AVG = 2,
    MEDIAN = 3,
} AggregateFunction;

#endif //AGGREGATE_FUNCTION_H
