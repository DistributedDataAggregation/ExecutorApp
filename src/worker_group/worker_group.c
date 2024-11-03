//
// Created by karol on 02.11.24.
//

#include <pthread.h>
#include "worker_group.h"
#include "thread_data.h"
#include "workers/worker.h"

#include <stdio.h>

#include "error_utilites.h"
#include <parquet-glib/arrow-file-reader.h>

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../parquet_helpers/parquet_helpers.h"
#define NUM_THREADS 4



ThreadData* get_thread_data(const QueryRequest* request, int thread_index, int num_threads,
    RowGroupsRange* row_groups_ranges, int* grouping_indices, int* select_indices);
void free_thread_data(ThreadData* thread_data);
AggregateFunction map_aggregate_function(Aggregate aggregate);
RowGroupsRange** get_row_group_ranges(int n_files, char** file_names, int num_threads);
int get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices) ;
void free_row_group_ranges(RowGroupsRange** row_group_ranges, int count);

HashTable** run_request_on_worker_group(const QueryRequest* request) {
    long threads_count = sysconf(_SC_NPROCESSORS_ONLN);;
    if (threads_count == -1) {
        REPORT_ERR("sysconf");
        return NULL;
    }

    pthread_t* threads = malloc(sizeof(pthread_t) * threads_count);
    if(threads == NULL) {
        REPORT_ERR("malloc");
        return NULL;
    }

    RowGroupsRange** row_group_ranges = get_row_group_ranges(request->n_files_names, request->files_names, threads_count);
    if(row_group_ranges == NULL) {
        REPORT_ERR("get_row_group_ranges");
        free(threads);
        return NULL;
    }

    int* grouping_indices = malloc(sizeof(int)*request->n_group_columns);
    if(grouping_indices == NULL) {
        REPORT_ERR("malloc");
        free(threads);
        free(row_group_ranges);
        return NULL;
    }

    int* select_indices = malloc(sizeof(int)*request->n_select);
    if(select_indices == NULL) {
        REPORT_ERR("malloc");
        free(threads);
        free(row_group_ranges);
        free(grouping_indices);
        return NULL;
    }

    if(!get_columns_indices(request, grouping_indices, select_indices)) {
        REPORT_ERR("get_columns_indices");
        free(threads);
        free(row_group_ranges);
        free(grouping_indices);
        free(select_indices);
        return NULL;
    }

    ThreadData** thread_data = malloc(sizeof(ThreadData*) * threads_count);
    for(int i = 0; i < threads_count; i++) {
        thread_data[i] = get_thread_data(request, i, threads_count, row_group_ranges[i], grouping_indices, select_indices);
        pthread_create(&threads[i], NULL, compute_on_thread, thread_data[i]);
    }

    for(int i = 0; i < threads_count; i++) {
        void* result = NULL;
        pthread_join(threads[i], &result);
        HashTable* thread_ht = result;
    }

    free(thread_data);
    free(threads);
    free(row_group_ranges);
    free(grouping_indices);
    free(select_indices);

    return NULL;
}

ThreadData* get_thread_data(const QueryRequest* request, int thread_index, int num_threads,
    RowGroupsRange* row_groups_ranges, int* grouping_indices, int* select_indices) {
    ThreadData* thread_data = malloc(sizeof(ThreadData));
    if (thread_data == NULL) {
        REPORT_ERR("malloc\n");
        return NULL;
    }

    thread_data->thread_index = thread_index;
    thread_data->num_threads = num_threads;

    thread_data->n_files = request->n_files_names;
    thread_data->n_group_columns = request->n_group_columns;
    thread_data->n_select = request->n_select;

    thread_data->file_names = (char**)malloc(sizeof(char*) * thread_data->n_files);
    if(thread_data->file_names == NULL) {
        REPORT_ERR("malloc\n");
        free_thread_data(thread_data);
        return NULL;
    }

    for(int i=0; i<thread_data->n_files; i++) {
        thread_data->file_names[i] = strdup(request->files_names[i]);
        if(thread_data->file_names[i] == NULL) {
            REPORT_ERR("strdup\n");
            free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->group_columns_indices = grouping_indices;

    thread_data->selects = malloc(sizeof(SelectData) * thread_data->n_select);
    if(thread_data->selects == NULL) {
        REPORT_ERR("malloc\n");
        free_thread_data(thread_data);
        return NULL;
    }

    for(int i=0; i<thread_data->n_select; i++) {
        Select* select = request->select[i];
        thread_data->selects[i].column_index = select_indices[i];
        thread_data->selects[i].aggregate = map_aggregate_function(select->function);
    }

    thread_data->file_row_groups_ranges = row_groups_ranges;

    return thread_data;
}

AggregateFunction map_aggregate_function(Aggregate aggregate) {
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

void free_thread_data(ThreadData* thread_data) {
    if(thread_data == NULL)
        return;

    if(thread_data->file_names != NULL) {
        for(int i=0; i<thread_data->n_files; i++) {
            free(thread_data->file_names[i]);
        }
        free(thread_data->file_names);
    }

    free(thread_data->group_columns_indices);
    free(thread_data->selects);
    free(thread_data);
}

RowGroupsRange** get_row_group_ranges(int n_files, char** file_names, int num_threads) {
    RowGroupsRange** row_group_ranges = malloc(sizeof(RowGroupsRange*) * num_threads);
    if(row_group_ranges == NULL) {
        REPORT_ERR("malloc\n");
        return NULL;
    }

    for(int i=0; i<num_threads; i++) {
        row_group_ranges[i] = malloc(sizeof(RowGroupsRange)*n_files);
        if(row_group_ranges[i] == NULL) {
            REPORT_ERR("malloc\n");
            free_row_group_ranges(row_group_ranges, i);
            return NULL;
        }
    }

    for(int i=0; i<n_files; i++) {
        GError* error = NULL;
        printf("File name: %s\n", file_names[i]);
        GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(file_names[i], &error);

        if(reader == NULL) {
            report_g_error(error);
            free_row_group_ranges(row_group_ranges, n_files);
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

void free_row_group_ranges(RowGroupsRange** row_group_ranges, int count) {
    if(row_group_ranges == NULL)
        return;

    for(int i=0; i<count; i++) {
        if(row_group_ranges[i] != NULL) {
            free(row_group_ranges[i]);
        }
    }

    free(row_group_ranges);
}

int get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices) {
    if(request->files_names == NULL || request->files_names[0] == NULL) {
        INTERNAL_ERROR("get_columns_indices: File names is NULL\n");
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
            INTERNAL_ERROR("get_columns_indices: Cannot find provided column name\n");
            fprintf(stderr, "Cannot find provided column: %s\n", request->group_columns[i]);
            return FALSE;
        }
    }

    for(int i=0; i<request->n_select; i++) {
        select_indices[i] = garrow_schema_get_field_index(schema, request->select[i]->column);
        if(select_indices[i] == -1) {
            INTERNAL_ERROR("get_columns_indices: Cannot find provided column name\n");
            fprintf(stderr, "Cannot find provided column: %s\n", request->select[i]->column);
            return FALSE;
        }
    }

    return TRUE;
}