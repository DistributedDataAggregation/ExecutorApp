//
// Created by karol on 02.11.24.
//

#ifndef WORKER_GROUP_H
#define WORKER_GROUP_H

#include <parquet-glib/arrow-file-reader.h>
#include "error_handling.h"
#include "hash_table.h"
#include "hash_table_interface.h"
#include "query_request.pb-c.h"
#include "thread_data.h"

#ifdef __cplusplus
extern "C" {
#endif
void worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table,
                              HashTableInterface* hash_table_interface, ErrorInfo* err);

ColumnDataType* worker_group_get_columns_data_types(const int* indices, int indices_count, const char* filename,
                                                    ErrorInfo* err);

ThreadData* worker_group_get_thread_data(const QueryRequest* request, int thread_index, int num_threads,
                                         RowGroupsRange* row_groups_ranges, int* grouping_indices,
                                         const int* select_indices,
                                         ColumnDataType* group_columns_types, ColumnDataType* select_columns_types,
                                         HashTableInterface* hash_table_interface, int hash_tables_max_size, ErrorInfo* err);

void worker_group_free_thread_data(ThreadData* thread_data);

RowGroupsRange** worker_group_get_row_group_ranges(int n_files, char** file_names, int num_threads, ErrorInfo* err);

void worker_group_get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices,
                                      ErrorInfo* err);

void worker_group_free_row_group_ranges(RowGroupsRange** row_group_ranges, int count);

ColumnDataType worker_group_map_arrow_data_type(GArrowDataType* data_type, ErrorInfo* err);

int* worker_group_hash_tables_max_size(RowGroupsRange** row_group_ranges, int num_threads, int num_files,
                                            ErrorInfo* err);


#ifdef __cplusplus
}
#endif

#endif //WORKER_GROUP_H
