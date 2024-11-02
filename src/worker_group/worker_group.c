//
// Created by karol on 02.11.24.
//

#include <pthread.h>
#include "worker_group.h"
#include "thread_data.h"
#include "workers/worker.h"

#include <stdio.h>

#include "error_utilites.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#define NUM_THREADS 4



ThreadData* get_thread_data(const QueryRequest* request, int thread_index, int num_threads);
void free_thread_data(ThreadData* thread_data);
AggregateFunction map_aggregate_function(Aggregate aggregate);

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
    // TODO: precompute row groups per file assigned to each thread
    // This formula solves it
    // for (int i = 0; i < threads; ++i) {
    //     int tasks_for_this_thread = tasks / threads + (i < tasks % threads);
    // }
    //
    for(int i = 0; i < threads_count; i++) {
        pthread_create(&threads[i], NULL, compute_on_thread, get_thread_data(request, i, threads_count));
    }

    for(int i = 0; i < threads_count; i++) {
        void* result = NULL;
        pthread_join(threads[i], &result);
        HashTable* thread_ht = result;
    }

    free(threads);

    return NULL;
}

ThreadData* get_thread_data(const QueryRequest* request, const int thread_index, const int num_threads) {
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