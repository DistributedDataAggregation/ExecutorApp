//
// Created by karol on 27.10.24.
//

#ifndef OPERATION_H
#define OPERATION_H

#define summation "sum"
#define minimum "min"
#define maximum "max"
#define average "avg"

enum operation {
    SUM,
    MIN,
    MAX,
    AVERAGE,
    UNDEFINED
};

typedef enum operation Operation;

#endif //OPERATION_H
