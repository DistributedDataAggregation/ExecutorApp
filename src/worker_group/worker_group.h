//
// Created by karol on 02.11.24.
//

#ifndef WORKER_GROUP_H
#define WORKER_GROUP_H

#include "hash_table.h"
#include "query_request.pb-c.h"
#include "thread_data.h"
#include <parquet-glib/arrow-file-reader.h>

#ifdef __cplusplus
extern "C" {
#endif
int worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table, ErrorInfo* err);
ColumnDataType* worker_group_get_columns_data_types(int* indices, int indices_count, const char* filename);
ThreadData* worker_group_get_thread_data(const QueryRequest* request, int thread_index, int num_threads,
    RowGroupsRange* row_groups_ranges, int* grouping_indices, int* select_indices, ColumnDataType* group_columns_types,
   ColumnDataType* select_columns_types);

void worker_group_free_thread_data(ThreadData* thread_data);

AggregateFunction worker_group_map_aggregate_function(Aggregate aggregate);

RowGroupsRange** worker_group_get_row_group_ranges(int n_files, char** file_names, int num_threads);

int worker_group_get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices) ;

void worker_group_free_row_group_ranges(RowGroupsRange** row_group_ranges, int count);

ColumnDataType worker_group_map_arrow_data_type(GArrowDataType* data_type);

#ifdef __cplusplus
}
#endif

#endif //WORKER_GROUP_H
