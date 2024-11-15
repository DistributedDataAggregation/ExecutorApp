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
ColumnDataType map_arrow_data_type(GArrowDataType* data_type);
ColumnDataType* get_columns_data_types(const int* indices, int indices_count, const char* filename);
void combine_hash_tables(HashTable* destination, HashTable* source);

HashTable* run_request_on_worker_group(const QueryRequest* request) {
    long threads_count = sysconf(_SC_NPROCESSORS_ONLN);

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

    HashTable* ht = NULL;
    for(int i = 0; i < threads_count; i++) {
        void* result = NULL;
        pthread_join(threads[i], &result);
        free_thread_data(thread_data[i]);
        HashTable* thread_ht = result;

        if(ht == NULL) {
            ht = thread_ht;
        }else {
            combine_hash_tables(ht, thread_ht);
            free_hash_table(thread_ht);
        }
    }

    free(threads);
    free(thread_data);
    free(select_indices);
    free(grouping_indices);
    free(row_group_ranges);

    return ht;
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

    thread_data->selects_indices = malloc(sizeof(int) * thread_data->n_select);
    if(thread_data->selects_indices == NULL) {
        REPORT_ERR("malloc\n");
        free_thread_data(thread_data);
        return NULL;
    }

    thread_data->selects_aggregate_functions = malloc(sizeof(AggregateFunction) * thread_data->n_select);
    if(thread_data->selects_aggregate_functions == NULL) {
        REPORT_ERR("malloc\n");
        free_thread_data(thread_data);
        return NULL;
    }

    for(int i=0; i<thread_data->n_select; i++) {
        Select* select = request->select[i];
        thread_data->selects_indices[i]= select_indices[i];
        thread_data->selects_aggregate_functions[i] = map_aggregate_function(select->function);
    }

    thread_data->file_row_groups_ranges = row_groups_ranges;

    // TODO: calculate the column data types outside of this function as its shared between threads
    // this is to avoid redundant calculations
    thread_data->group_columns_data_types = get_columns_data_types(
        thread_data->group_columns_indices,
        thread_data->n_group_columns,
        thread_data->file_names[0]);

    thread_data->select_columns_types = get_columns_data_types(
        thread_data->selects_indices,
        thread_data->n_select,
        thread_data->file_names[0]);

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
            if(thread_data->file_names[i] != NULL) {
                free(thread_data->file_names[i]);
            }
        }
        free(thread_data->file_names);
    }

    free(thread_data->selects_aggregate_functions);
    free(thread_data->file_row_groups_ranges);
    free(thread_data->group_columns_data_types);
    free(thread_data->select_columns_types);
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

ColumnDataType* get_columns_data_types(const int* indices, int indices_count, const char* filename) {
    ColumnDataType* data_types = malloc(sizeof(ColumnDataType) * indices_count);

    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(filename, &error);
    if(reader == NULL) {
        report_g_error(error);
        return NULL;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if(schema == NULL) {
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

        data_types[i] = map_arrow_data_type(data_type);
        g_object_unref(field);
    }

    g_object_unref(schema);
    g_object_unref(reader);

    return data_types;
}

ColumnDataType map_arrow_data_type(GArrowDataType* data_type) {
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

void combine_hash_tables(HashTable* destination, HashTable* source) {
    for(int i=0;i<source->size;i++) {
        HashTableEntry* entry = source->table[i];
        while(entry != NULL) {
            HashTableEntry* next = entry->next;

            HashTableEntry* found = search(destination, entry->key);
            if(found == NULL) {
                entry->next = NULL;
                insert(destination, next);
            } else {
                combine_entries(found, entry);
            }

            entry = next;
        }
    }
}