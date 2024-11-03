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



ThreadData* get_thread_data(const QueryRequest* request, int thread_index, int num_threads, RowGroupsRange* row_groups_ranges);
void free_thread_data(ThreadData* thread_data);
AggregateFunction map_aggregate_function(Aggregate aggregate);
RowGroupsRange** get_row_group_ranges(int n_files, char** file_names, int num_threads);
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

    for(int i = 0; i < threads_count; i++) {
        pthread_create(&threads[i], NULL, compute_on_thread, get_thread_data(request, i, threads_count, row_group_ranges[i]));
    }

    for(int i = 0; i < threads_count; i++) {
        void* result = NULL;
        pthread_join(threads[i], &result);
        HashTable* thread_ht = result;
    }

    free(threads);

    return NULL;
}

ThreadData* get_thread_data(const QueryRequest* request, int thread_index, int num_threads, RowGroupsRange* row_groups_ranges) {
    ThreadData* thread_data = malloc(sizeof(ThreadData));
    if (thread_data == NULL) {
        REPORT_ERR("malloc\n");
        return thread_data;
    }

    thread_data->thread_index = thread_index;
    thread_data->num_threads = num_threads;

    thread_data->n_files = request->n_files_names;
    thread_data->n_group_columns = request->n_group_columns;
    thread_data->n_select = request->n_select;

    thread_data->file_names = (char**)malloc(sizeof(char*) * thread_data->n_files);
    if(thread_data->file_names == NULL) {
        REPORT_ERR("malloc\n");
    }

    for(int i=0; i<thread_data->n_files; i++) {
        thread_data->file_names[i] = strdup(request->files_names[i]);
        if(thread_data->file_names[i] == NULL) {
            REPORT_ERR("strdup\n");
            free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->group_columns = malloc(sizeof(char*) * thread_data->n_group_columns);
    for(int i=0; i<thread_data->n_group_columns; i++) {
        thread_data->group_columns[i] = strdup(request->group_columns[i]);
        if(thread_data->group_columns[i] == NULL) {
            REPORT_ERR("strdup\n");
            free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->selects = malloc(sizeof(SelectData) * thread_data->n_select);
    for(int i=0; i<thread_data->n_select; i++) {
        Select* select = request->select[i];
        thread_data->selects[i].column = strdup(select->column);
        if(thread_data->selects[i].column == NULL) {
            REPORT_ERR("strdup");
            free_thread_data(thread_data);
            return NULL;
        }

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

    if(thread_data->group_columns != NULL) {
        for(int i=0; i<thread_data->n_group_columns; i++) {
            free(thread_data->group_columns[i]);
        }
        free(thread_data->group_columns);
    }

    if(thread_data->selects != NULL) {
        for(int i=0; i<thread_data->n_select; i++) {
            free(thread_data->selects[i].column);
        }
        free(thread_data->selects);
    }

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
