//
// Created by karol on 10.11.24.
//

#ifndef AGGREGATEFUNCTION_H
#define AGGREGATEFUNCTION_H

typedef enum aggregate_function {
    UNKNOWN = -1,
    MIN = 0,
    MAX = 1,
    AVG = 2,
    MEDIAN = 3,
} AggregateFunction;

#endif //AGGREGATEFUNCTION_H
