//
// Created by karol on 02.11.24.
//
#ifndef THREAD_DATA_H
#define THREAD_DATA_H
#include "aggregate_function.h"
#include "hash_table_interface.h"


typedef enum ColumnDataType
{
    COLUMN_DATA_TYPE_INT32,
    COLUMN_DATA_TYPE_INT64,
    COLUMN_DATA_TYPE_STRING,
    COLUMN_DATA_TYPE_FLOAT,
    COLUMN_DATA_TYPE_DOUBLE,
    COLUMN_DATA_TYPE_BOOLEAN,
    COLUMN_DATA_TYPE_UNKNOWN
} ColumnDataType;

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
    int* group_columns_indices;
    ColumnDataType* group_columns_data_types;

    int n_select;
    int* selects_indices;
    AggregateFunction* selects_aggregate_functions;
    ColumnDataType* select_columns_types;

    HashTableInterface* ht_interface;
    int ht_max_size;

    ErrorInfo* thread_error;
} ThreadData;

#endif //THREAD_DATA_H
