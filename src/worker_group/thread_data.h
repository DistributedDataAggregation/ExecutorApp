//
// Created by karol on 02.11.24.
//
#ifndef THREAD_DATA_H
#define THREAD_DATA_H
#include "aggregate_function.h"


typedef enum ColumnDataType
{
    COLUMN_DATA_TYPE_INT32,
    COLUMN_DATA_TYPE_INT64,
    COLUMN_DATA_TYPE_STRING,
    COLUMN_DATA_TYPE_UNKNOWN
} ColumnDataType;


//
// typedef struct SelectData {
//     int column_index;
//     AggregateFunction aggregate;
// } SelectData;

typedef struct RowGroupsRange
{
    int start;
    int count;
} RowGroupsRange;

typedef struct thread_data
{
    int num_threads;
    int thread_index;

    int n_files;
    char** file_names;
    RowGroupsRange* file_row_groups_ranges;

    int n_group_columns;
    ColumnDataType* group_columns_data_types;

    int n_select;
    AggregateFunction* selects_aggregate_functions;
    ColumnDataType* select_columns_types;

    int* columns_indices;
    int* columns_non_repeating_mapping;
} ThreadData;

#endif //THREAD_DATA_H
