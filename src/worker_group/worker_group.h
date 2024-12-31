//
// Created by karol on 02.11.24.
//

#ifndef WORKER_GROUP_H
#define WORKER_GROUP_H

#include <parquet-glib/arrow-file-reader.h>
#include "error_handling.h"
#include "hash_table.h"
#include "query_request.pb-c.h"
#include "thread_data.h"

#ifdef __cplusplus
extern "C" {
#endif
void worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table, ErrorInfo* err);

ColumnDataType* worker_group_get_columns_data_types(const int* indices, int indices_size, int indices_count,
        int offset_from_start, const char* filename, ErrorInfo* err);

ThreadData* worker_group_get_thread_data(const QueryRequest* request, const int thread_index, const int num_threads,
                                             RowGroupsRange* row_groups_ranges, int* columns_indices, int* columns_non_repeating_mappings,
                                             ColumnDataType* group_columns_types, ColumnDataType* select_columns_types,
                                             ErrorInfo* err);

void worker_group_free_thread_data(ThreadData* thread_data);

AggregateFunction worker_group_map_aggregate_function(Aggregate aggregate, ErrorInfo* err);

RowGroupsRange** worker_group_get_row_group_ranges(int n_files, char** file_names, int num_threads, ErrorInfo* err);

    void worker_group_get_columns_indices(const QueryRequest* request, int* column_indices, ErrorInfo* err);

void worker_group_free_row_group_ranges(RowGroupsRange** row_group_ranges, int count);

ColumnDataType worker_group_map_arrow_data_type(GArrowDataType* data_type, ErrorInfo* err);

void worker_group_calculate_new_column_indices(int* new_column_indices, const gint* old_column_indices,
                                         const int number_of_columns);

#ifdef __cplusplus
}
#endif

#endif //WORKER_GROUP_H
