//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_H
#define REQUEST_H

#include "aggregation.h"

struct request {
    int files_count;
    char** files;

    int grouping_columns_count;
    char** grouping_columns;

    int aggregation_columns_count;
    Aggregation* aggregation;
};

typedef struct request Request;

#endif //REQUEST_H
