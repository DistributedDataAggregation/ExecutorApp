//
// Created by karol on 31.10.24.
//

#ifndef WORKER_H
#define WORKER_H

#include <parquet-glib/arrow-file-reader.h>
#include "error_handling.h"
#include "hash_table.h"
#include "thread_data.h"

#ifdef __cplusplus
extern "C" {
#endif

void* worker_compute_on_thread(void* arg);

void worker_print_thread_data(ThreadData* data);

void worker_compute_file(int index_of_the_file, const ThreadData* data, HashTable* hash_table, ErrorInfo* err);

char* worker_get_grouping_string(GArrowArray* grouping_array, ColumnDataType data_type, int row_index, ErrorInfo* err);

char* worker_construct_grouping_string(int n_group_columns, GArrowArray** grouping_arrays, int row_index,
                                const ColumnDataType* group_columns_data_types, ErrorInfo* err);

HashTableValue worker_get_hash_table_value(GArrowArray* select_array, int row_index, ColumnDataType select_columns_data_types,
                                    AggregateFunction aggregate_function, ErrorInfo* err);

void worker_calculate_new_column_indices(int* new_column_indices, const gint* old_column_indices,
                                         int number_of_columns);

HashTableValueType worker_map_column_data_type(ColumnDataType column_data_type);

void worker_free_resources(GParquetArrowFileReader* reader, GArrowTable* table,
                           GArrowChunkedArray** grouping_chunked_arrays, int n_group_columns,
                           GArrowChunkedArray** select_chunked_arrays, int n_select,
                           GArrowArray** grouping_arrays, GArrowArray** select_arrays,
                           gint* columns_indices, int* new_columns_indices);

#ifdef __cplusplus
}
#endif

#endif //WORKER_H
