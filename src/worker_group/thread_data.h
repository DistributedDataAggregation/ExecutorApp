//
// Created by karol on 02.11.24.
//

#ifndef THREAD_DATA_H
#define THREAD_DATA_H

typedef enum AggregateFunction {
    UNKNOWN = -1,
    MIN = 0,
    MAX = 1,
    AVG = 2,
    MEDIAN = 3,
} AggregateFunction;

typedef struct SelectData {
    char* column;
    AggregateFunction aggregate;
} SelectData;

typedef struct RowGroupsRange{
    int start;
    int count;
} RowGroupsRange;

typedef struct thread_data {
    int num_threads;
    int thread_index;

    int n_files;
    char** file_names;
    RowGroupsRange* file_row_groups_ranges;

    int n_group_columns;
    char** group_columns;

    int n_select;
    SelectData* selects;
} ThreadData;

#endif //THREAD_DATA_H
