//
// Created by karol on 02.11.24.
//

#include <pthread.h>
#include "worker_group.h"

#include <error.h>

#include "thread_data.h"
#include "workers/worker.h"

#include <stdio.h>

#include "error_utilites.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../parquet_helpers/parquet_helpers.h"
#define NUM_THREADS 4


// TODO() probably split this function
int worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table) {
    if(!request) {
        return -1;
    }

    long threads_count = sysconf(_SC_NPROCESSORS_ONLN);
    if (threads_count == -1) {
        REPORT_ERR("sysconf");
        return -1;
    }

    pthread_t* threads = (pthread_t*) malloc(sizeof(pthread_t) * threads_count);
    if(NULL == threads ) {
        REPORT_ERR("malloc");
        return -1;
    }

    RowGroupsRange** row_group_ranges = worker_group_get_row_group_ranges(request->n_files_names, request->files_names, threads_count);
    if(NULL == row_group_ranges)  {
        REPORT_ERR("worker_group_get_row_group_ranges");
        free(threads);
        return -1;
    }

    int* grouping_indices = (int*)malloc(sizeof(int)*request->n_group_columns);
    if(grouping_indices == NULL) {
        REPORT_ERR("malloc");
        free(threads);
        free(row_group_ranges);
        return -1;
    }

    int* select_indices = (int*)malloc(sizeof(int)*request->n_select);
    if(select_indices == NULL) {
        REPORT_ERR("malloc");
        free(threads);
        free(row_group_ranges);
        return -1;
    }

    if(!worker_group_get_columns_indices(request, grouping_indices, select_indices)) {
        REPORT_ERR("worker_group_get_columns_indices");
        free(threads);
        free(row_group_ranges);
        free(grouping_indices);
        free(select_indices);
        return -1;
    }

    ColumnDataType * grouping_columns_data_type = worker_group_get_columns_data_types(grouping_indices, request->n_group_columns,
        request->files_names[0]);
    ColumnDataType * select_columns_data_type = worker_group_get_columns_data_types(select_indices, request->n_select,
        request->files_names[0]);

    ThreadData** thread_data = (ThreadData**)malloc(sizeof(ThreadData*) * threads_count);
    for(int i = 0; i < threads_count; i++) {

        thread_data[i] = worker_group_get_thread_data(request, i, threads_count, row_group_ranges[i],
            grouping_indices, select_indices,grouping_columns_data_type, select_columns_data_type);

        pthread_create(&threads[i], NULL, compute_on_thread, thread_data[i]);
    }

    for(int i = 0; i < threads_count; i++) {
        void* result = NULL;
        pthread_join(threads[i], &result);
        worker_group_free_thread_data(thread_data[i]);
        HashTable* thread_ht = (HashTable*) result;

        if (*request_hash_table == NULL) {
            *request_hash_table = thread_ht;
        } else {
            hash_table_combine_hash_tables(*request_hash_table, thread_ht);
            hash_table_free(thread_ht);
        }
    }

    free(threads);
    free(select_indices);
    free(grouping_columns_data_type);
    free(select_columns_data_type);
    free(grouping_indices);
    free(thread_data);
    free(row_group_ranges);

    return 0;
}

ThreadData* worker_group_get_thread_data(const QueryRequest* request, int thread_index, int num_threads,
    RowGroupsRange* row_groups_ranges, int* grouping_indices, int* select_indices, ColumnDataType* group_columns_types,
    ColumnDataType* select_columns_types) {

    if(request == NULL || thread_index < 0 || thread_index >= num_threads) {
        REPORT_ERR("worker_group_get_thread_data");
        return NULL;
    }

    ThreadData* thread_data = (ThreadData*) malloc(sizeof(ThreadData));
    if (thread_data == NULL) {
        REPORT_ERR("malloc\n");
        return NULL;
    }
    thread_data->file_row_groups_ranges = row_groups_ranges;
    thread_data->group_columns_data_types = group_columns_types;
    thread_data->select_columns_types = select_columns_types;

    thread_data->thread_index = thread_index;
    thread_data->num_threads = num_threads;

    thread_data->n_files = request->n_files_names;
    thread_data->n_group_columns = request->n_group_columns;
    thread_data->n_select = request->n_select;

    thread_data->file_names = (char**)malloc(sizeof(char*) * thread_data->n_files);
    if(thread_data->file_names == NULL) {
        REPORT_ERR("malloc\n");
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

    for(int i=0; i<thread_data->n_files; i++) {
        thread_data->file_names[i] = strdup(request->files_names[i]);
        if(thread_data->file_names[i] == NULL) {
            REPORT_ERR("strdup\n");
            worker_group_free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->group_columns_indices = grouping_indices;

    thread_data->selects_indices = (int*)malloc(sizeof(int) * thread_data->n_select);
    if(thread_data->selects_indices == NULL) {
        REPORT_ERR("malloc\n");
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

    thread_data->selects_aggregate_functions = (AggregateFunction*)malloc(sizeof(AggregateFunction) * thread_data->n_select);
    if(thread_data->selects_aggregate_functions == NULL) {
        REPORT_ERR("malloc\n");
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

    for(int i=0; i<thread_data->n_select; i++) {
        Select* select = request->select[i];
        thread_data->selects_indices[i]= select_indices[i];
        thread_data->selects_aggregate_functions[i] = worker_group_map_aggregate_function(select->function);
    }

    return thread_data;
}

AggregateFunction worker_group_map_aggregate_function(Aggregate aggregate) {
    switch (aggregate) {
        case AGGREGATE__Minimum:
            return MIN;
        case AGGREGATE__Maximum:
            return MAX;
        case AGGREGATE__Average:
            return AVG;
        case AGGREGATE__Median:
            return MEDIAN;
        default:
            INTERNAL_ERROR("Unknown Aggregate function, can't map to AggregateFunction type\n");
            return UNKNOWN;
    }
}

void worker_group_free_thread_data(ThreadData* thread_data) {
    if(thread_data == NULL)
        return;

    if(thread_data->file_names != NULL) {
        for(int i=0; i<thread_data->n_files; i++) {
            if(thread_data->file_names[i] != NULL) {
                free(thread_data->file_names[i]);
            }
        }
        free(thread_data->file_names);
    }

    free(thread_data->selects_aggregate_functions);
    free(thread_data->file_row_groups_ranges);
    free(thread_data);
}

RowGroupsRange** worker_group_get_row_group_ranges(int n_files, char** file_names, int num_threads) {
    RowGroupsRange** row_group_ranges = (RowGroupsRange**) malloc(sizeof(RowGroupsRange*) * num_threads);
    if(row_group_ranges == NULL) {
        REPORT_ERR("malloc\n");
        return NULL;
    }

    for(int i=0; i<num_threads; i++) {
        row_group_ranges[i] = (RowGroupsRange*) malloc(sizeof(RowGroupsRange)*n_files);
        if(row_group_ranges[i] == NULL) {
            REPORT_ERR("malloc\n");
               worker_group_free_row_group_ranges(row_group_ranges, i);
            return NULL;
        }
    }

    for(int i=0; i<n_files; i++) {
        GError* error = NULL;
        printf("File name: %s\n", file_names[i]);
        GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(file_names[i], &error);

        if(reader == NULL) {
            report_g_error(error);
            worker_group_free_row_group_ranges(row_group_ranges, n_files);
            return NULL;
        }

        gint row_groups_count = gparquet_arrow_file_reader_get_n_row_groups(reader);

        int start = 0;
        for(int j=0 ;j <num_threads; j++) {
            int count_for_thread_j = row_groups_count / num_threads + (j < row_groups_count % num_threads);
            row_group_ranges[j][i].start = start;
            row_group_ranges[j][i].count = count_for_thread_j;
            start += count_for_thread_j;
        }
        g_object_unref(reader);
    }

    return row_group_ranges;
}

void worker_group_free_row_group_ranges(RowGroupsRange** row_group_ranges, int count) {
    if(row_group_ranges == NULL)
        return;

    for(int i=0; i<count; i++) {
        if(row_group_ranges[i] != NULL) {
            free(row_group_ranges[i]);
        }
    }

    free(row_group_ranges);
}

int worker_group_get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices) {
    if(request->files_names == NULL || request->files_names[0] == NULL) {
        INTERNAL_ERROR("worker_group_get_columns_indices: File names is NULL\n");
        return FALSE;
    }

    GError* error = NULL;
    // i assume all files have the same schema
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(request->files_names[0], &error);
    if(reader == NULL) {
        report_g_error(error);
        return FALSE;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if(schema == NULL) {
        report_g_error(error);
        return FALSE;
    }

    for(int i=0; i<request->n_group_columns; i++) {
        grouping_indices[i] = garrow_schema_get_field_index(schema, request->group_columns[i]);
        if(grouping_indices[i] == -1) {
            INTERNAL_ERROR("worker_group_get_columns_indices: Cannot find provided column name\n");
            fprintf(stderr, "Cannot find provided column: %s\n", request->group_columns[i]);
            return FALSE;
        }
    }

    for(int i=0; i<request->n_select; i++) {
        select_indices[i] = garrow_schema_get_field_index(schema, request->select[i]->column);
        if(select_indices[i] == -1) {
            INTERNAL_ERROR("worker_group_get_columns_indices: Cannot find provided column name\n");
            fprintf(stderr, "Cannot find provided column: %s\n", request->select[i]->column);
            return FALSE;
        }
    }

    return TRUE;
}

ColumnDataType* worker_group_get_columns_data_types(int* indices, int indices_count, const char* filename) {
    ColumnDataType* data_types = malloc(sizeof(ColumnDataType) * indices_count);
    if(!data_types) {
        return NULL ;
    }

    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(filename, &error);
    if(reader == NULL) {
        report_g_error(error);
        free(data_types);
        return NULL;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if(schema == NULL) {
        free(data_types);
        report_g_error(error);
        return NULL;
    }

    for(int i=0; i<indices_count; i++) {
        GArrowField* field = garrow_schema_get_field(schema, indices[i]);
        if(field == NULL) {
            report_g_error(error);
            free(data_types);
            g_object_unref(field);
            g_object_unref(schema);
            g_object_unref(reader);
            return NULL;
        }

        GArrowDataType* data_type = garrow_field_get_data_type(field);

        gchar* data_type_string = garrow_data_type_to_string(data_type);
        printf("Column %d has datatype %s\n", i, data_type_string);
        g_free(data_type_string);

        data_types[i] = worker_group_map_arrow_data_type(data_type);
        g_object_unref(field);
    }

    g_object_unref(schema);
    g_object_unref(reader);


    return data_types;
}

ColumnDataType worker_group_map_arrow_data_type(GArrowDataType* data_type) {
    if(GARROW_IS_INT32_DATA_TYPE(data_type) ) {
        return COLUMN_DATA_TYPE_INT32;
    }

    if (GARROW_IS_INT64_DATA_TYPE(data_type)) {
        return COLUMN_DATA_TYPE_INT64;
    }

    if(GARROW_IS_STRING_DATA_TYPE(data_type)) {
        return COLUMN_DATA_TYPE_STRING;
    }
    return COLUMN_DATA_TYPE_UNKNOWN;
}

