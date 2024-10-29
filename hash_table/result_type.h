//
// Created by karol on 29.10.24.
//

#ifndef RESULT_H
#define RESULT_H

struct singleResult {
    long value;
};
typedef struct singleResult SingleResult;

struct countedResult {
    long value;
    long count;
};
typedef struct countedResult CountedResult;

union resultType {
    SingleResult singleResult;
    CountedResult countedResult;
};

typedef union resultType ResultType;

#endif //RESULT_H
