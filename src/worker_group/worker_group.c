//
// Created by karol on 02.11.24.
//

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "../parquet_helpers/parquet_helpers.h"
#include "thread_data.h"
#include "worker_group.h"

#include "logging.h"
#include "workers/worker.h"

#define NUM_THREADS 4

// TODO() probably split this function
void worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }

    if (request == NULL)
    {
        LOG_INTERNAL_ERR("Failed to run worker group: Request was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Failed to run worker group", "Request was NULL");
        return;
    }

    const int threads_count = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (threads_count == -1)
    {
        LOG_ERR("Failed to get the number of processors on the system");
        SET_ERR(err, errno, "Failed to get the number of processors on the system", strerror(errno));
        return;
    }

    pthread_t* threads = (pthread_t*)malloc(sizeof(pthread_t) * threads_count);
    if (threads == NULL)
    {
        LOG_ERR("Failed to allocate memory for the threads");
        SET_ERR(err, errno, "Failed to allocate memory for the threads", strerror(errno));
        return;
    }

    RowGroupsRange** row_group_ranges = worker_group_get_row_group_ranges((int)request->n_files_names,
                                                                          request->files_names, threads_count, err);
    if (err->error_code != NO_ERROR)
    {
        free(threads);
        free(row_group_ranges);
        return;
    }

    const int total_columns_count = (int)request->n_group_columns+ (int)request->n_select;

    int* column_indices = (int*)malloc(sizeof(int) * total_columns_count);
    if (column_indices == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping indices");
        SET_ERR(err, errno, "Failed to allocate memory for grouping indices", strerror(errno));
        free(threads);
        free(row_group_ranges);
        return;
    }

    worker_group_get_columns_indices(request, column_indices, err);
    if (err->error_code != NO_ERROR)
    {
        free(threads);
        free(row_group_ranges);
        free(column_indices);
        return;
    }

    int* columns_non_repeating_mappings = (int*)malloc(sizeof(int) * total_columns_count);
    if (columns_non_repeating_mappings == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping indices");
        SET_ERR(err, errno, "Failed to allocate memory for grouping indices", strerror(errno));
        free(threads);
        free(row_group_ranges);
        free(column_indices);
        return;
    }
    worker_group_calculate_new_column_indices(columns_non_repeating_mappings, column_indices, total_columns_count);

    ColumnDataType* grouping_columns_data_type = worker_group_get_columns_data_types(column_indices,
        request->n_group_columns + request->n_select,
        request->n_group_columns, 0, request->files_names[0], err);

    ColumnDataType* select_columns_data_type = worker_group_get_columns_data_types(column_indices,
        request->n_group_columns + request->n_select,
        request->n_select, request->n_group_columns,  request->files_names[0], err);

    if (err->error_code != NO_ERROR)
    {
        free(threads);
        free(row_group_ranges);
        free(column_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }

    ThreadData** thread_data = (ThreadData**)malloc(sizeof(ThreadData*) * threads_count);
    if (thread_data == NULL)
    {
        LOG_ERR("Failed to allocate memory for thread data");
        SET_ERR(err, errno, "Failed to allocate memory for thread data", strerror(errno));
        free(threads);
        free(row_group_ranges);
        free(column_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }

    for (int i = 0; i < threads_count; i++)
    {
        thread_data[i] = worker_group_get_thread_data(request, i, threads_count, row_group_ranges[i],
                                                      column_indices, columns_non_repeating_mappings,
                                                      grouping_columns_data_type,
                                                      select_columns_data_type, err);
        if (err->error_code != NO_ERROR)
        {
            for (int j = 0; j < i; j++)
            {
                worker_group_free_thread_data(thread_data[j]);
            }
            free(threads);
            free(row_group_ranges);
            free(grouping_columns_data_type);
            free(select_columns_data_type);
            free(thread_data);
            return;
        }

        const int ret = pthread_create(&threads[i], NULL, compute_on_thread, thread_data[i]);
        if (ret != 0)
        {
            LOG_ERR("Failed to create worker group thread");
            SET_ERR(err, ret, "Failed to create worker group thread", strerror(ret));
            for (int j = 0; j < i; j++)
            {
                worker_group_free_thread_data(thread_data[j]);
            }
            free(threads);
            free(row_group_ranges);
            free(grouping_columns_data_type);
            free(select_columns_data_type);
            free(thread_data);
            return;
        }
    }

    for (int i = 0; i < threads_count; i++)
    {
        void* result = NULL;
        const int ret = pthread_join(threads[i], &result);
        if (ret != 0)
        {
            LOG_ERR("Failed to join worker group thread");
            SET_ERR(err, ret, "Failed to join worker group thread", strerror(ret));
            // TODO handle exit?? or we need to wait for other thread anyway (now)
        }
        worker_group_free_thread_data(thread_data[i]);
        HashTable* thread_ht = (HashTable*)result;

        if (*request_hash_table == NULL)
        {
            *request_hash_table = thread_ht;
        }
        else
        {
            hash_table_combine_hash_tables(*request_hash_table, thread_ht, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_INTERNAL_ERR("Failed to run worker group: Failed to combine hash tables");
                SET_ERR(err, INTERNAL_ERROR, "Failed to run worker group", "Failed to combine hash tables");
                // TODO handle exit?? or we need to wait for other threads anyway (now)
            }
            hash_table_free(thread_ht);
        }
    }

    free(threads);
    free(row_group_ranges);
    free(grouping_columns_data_type);
    free(select_columns_data_type);
    free(thread_data);
}

ThreadData* worker_group_get_thread_data(const QueryRequest* request, const int thread_index, const int num_threads,
                                             RowGroupsRange* row_groups_ranges, int* columns_indices, int* columns_non_repeating_mappings,
                                             ColumnDataType* group_columns_types, ColumnDataType* select_columns_types,
                                             ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    if (request == NULL || thread_index < 0 || thread_index >= num_threads)
    {
        LOG_INTERNAL_ERR("Failed to get thread data: Invalid arguments passed");
        SET_ERR(err, INTERNAL_ERROR, "Failed to get thread data", "Invalid arguments passed");
        return NULL;
    }

    ThreadData* thread_data = (ThreadData*)malloc(sizeof(ThreadData));
    if (thread_data == NULL)
    {
        LOG_ERR("Failed to allocate memory for thread data");
        SET_ERR(err, errno, "Failed to allocate memory for thread data", strerror(errno));
        return NULL;
    }

    thread_data->file_row_groups_ranges = row_groups_ranges;
    thread_data->group_columns_data_types = group_columns_types;
    thread_data->select_columns_types = select_columns_types;

    thread_data->thread_index = thread_index;
    thread_data->num_threads = num_threads;

    thread_data->n_files = (int)request->n_files_names;
    thread_data->n_group_columns = (int)request->n_group_columns;
    thread_data->n_select = (int)request->n_select;

    thread_data->file_names = (char**)malloc(sizeof(char*) * thread_data->n_files);
    if (thread_data->file_names == NULL)
    {
        LOG_ERR("Failed to allocate memory for files names");
        SET_ERR(err, errno, "Failed to allocate memory for files names", strerror(errno));
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

    for (int i = 0; i < thread_data->n_files; i++)
    {
        thread_data->file_names[i] = strdup(request->files_names[i]);
        if (thread_data->file_names[i] == NULL)
        {
            LOG_ERR("Failed to duplicate file name");
            SET_ERR(err, errno, "Failed to duplicate file name", strerror(errno));
            worker_group_free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->columns_indices = columns_indices;

    thread_data->selects_aggregate_functions = (AggregateFunction*)malloc(
        sizeof(AggregateFunction) * thread_data->n_select);
    if (thread_data->selects_aggregate_functions == NULL)
    {
        LOG_ERR("Failed to allocate memory for selects aggregate functions");
        SET_ERR(err, errno, "Failed to allocate memory for selects aggregate functions", strerror(errno));
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

    for (int i = 0; i < thread_data->n_select; i++)
    {
        const Select* select = request->select[i];
        thread_data->selects_aggregate_functions[i] = worker_group_map_aggregate_function(select->function, err);
        if (err->error_code != NO_ERROR)
        {
            worker_group_free_thread_data(thread_data);
            return NULL;
        }
    }

    thread_data->columns_non_repeating_mapping = columns_non_repeating_mappings;

    return thread_data;
}

AggregateFunction worker_group_map_aggregate_function(Aggregate aggregate, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return UNKNOWN;
    }

    switch (aggregate)
    {
    case AGGREGATE__Minimum:
        return MIN;
    case AGGREGATE__Maximum:
        return MAX;
    case AGGREGATE__Average:
        return AVG;
    case AGGREGATE__Median:
        return MEDIAN;
    default:
        LOG_INTERNAL_ERR("Unsupported aggregate function");
        SET_ERR(err, INTERNAL_ERROR, "Unsupported aggregate function", "");
        return UNKNOWN;
    }
}

void worker_group_free_thread_data(ThreadData* thread_data)
{
    if (thread_data == NULL)
        return;

    if (thread_data->file_names != NULL)
    {
        for (int i = 0; i < thread_data->n_files; i++)
        {
            if (thread_data->file_names[i] != NULL)
            {
                free(thread_data->file_names[i]);
            }
        }
        free(thread_data->file_names);
    }

    free(thread_data->selects_aggregate_functions);
    free(thread_data->file_row_groups_ranges);
    free(thread_data->columns_indices);
    free(thread_data->columns_non_repeating_mapping);
    free(thread_data);

    thread_data = NULL;
}

RowGroupsRange** worker_group_get_row_group_ranges(const int n_files, char** file_names, const int num_threads,
                                                   ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    RowGroupsRange** row_group_ranges = (RowGroupsRange**)malloc(sizeof(RowGroupsRange*) * num_threads);
    if (row_group_ranges == NULL)
    {
        LOG_ERR("Failed to allocate memory for row_group_ranges");
        SET_ERR(err, errno, "Failed to allocate memory for row_group_ranges", strerror(errno));
        return NULL;
    }

    for (int i = 0; i < num_threads; i++)
    {
        row_group_ranges[i] = (RowGroupsRange*)malloc(sizeof(RowGroupsRange) * n_files);
        if (row_group_ranges[i] == NULL)
        {
            LOG_ERR("Failed to allocate memory for row_group_ranges");
            SET_ERR(err, errno, "Failed to allocate memory for row_group_ranges", strerror(errno));
            worker_group_free_row_group_ranges(row_group_ranges, i);
            return NULL;
        }
    }

    for (int i = 0; i < n_files; i++)
    {
        GError* error = NULL;
        LOG("File name: %s\n", file_names[i]);
        GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(file_names[i], &error);

        if (reader == NULL)
        {
            report_g_error(error, err, "Failed to open file");
            worker_group_free_row_group_ranges(row_group_ranges, n_files);
            return NULL;
        }

        const gint row_groups_count = gparquet_arrow_file_reader_get_n_row_groups(reader);

        int start = 0;
        for (int j = 0; j < num_threads; j++)
        {
            int count_for_thread_j = row_groups_count / num_threads + (j < row_groups_count % num_threads);
            row_group_ranges[j][i].start = start;
            row_group_ranges[j][i].count = count_for_thread_j;
            start += count_for_thread_j;
        }
        g_object_unref(reader);
    }

    return row_group_ranges;
}

void worker_group_free_row_group_ranges(RowGroupsRange** row_group_ranges, const int count)
{
    if (row_group_ranges == NULL)
        return;

    for (int i = 0; i < count; i++)
    {
        if (row_group_ranges[i] != NULL)
        {
            free(row_group_ranges[i]);
        }
    }

    free(row_group_ranges);
}

void worker_group_get_columns_indices(const QueryRequest* request, int* column_indices,
                                      ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }

    if (request->files_names == NULL || request->files_names[0] == NULL)
    {
        LOG_INTERNAL_ERR("Failed to get columns indices: Files names were NULL");
        SET_ERR(err, INTERNAL_ERROR, "Failed to get columns indices", "Files names were NULL");
        return;
    }

    GError* error = NULL;
    // i assume all files have the same schema
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(request->files_names[0], &error);
    if (reader == NULL)
    {
        report_g_error(error, err, "Failed to open file");
        return;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if (schema == NULL)
    {
        report_g_error(error, err, "Failed to get schema");
        return;
    }

    for (int i = 0; i < request->n_group_columns; i++)
    {
        column_indices[i] = garrow_schema_get_field_index(schema, request->group_columns[i]);
        if (column_indices[i] == -1)
        {
            LOG_INTERNAL_ERR("Failed to get grouping indices: Cannot find provided column name");
            fprintf(stderr, "Cannot find provided column: %s\n", request->group_columns[i]);
            SET_ERR(err, INTERNAL_ERROR, "Failed to get grouping indices", "Cannot find provided column name");
            return;
        }
    }

    for (int i = 0; i < request->n_select; i++)
    {
        column_indices[i+request->n_group_columns] = garrow_schema_get_field_index(schema, request->select[i]->column);
        if (column_indices[i+request->n_group_columns] == -1)
        {
            LOG_INTERNAL_ERR("Failed to get select indices: Cannot find provided column name");
            fprintf(stderr, "Cannot find provided column: %s\n", request->select[i]->column);
            SET_ERR(err, INTERNAL_ERROR, "Failed to get select indices", "Cannot find provided column name");
            return;
        }
    }
}

ColumnDataType* worker_group_get_columns_data_types(const int* indices, int indices_size, int indices_count,
    int offset_from_start, const char* filename, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    ColumnDataType* data_types = malloc(sizeof(ColumnDataType) * indices_count);
    if (data_types == NULL)
    {
        LOG_ERR("Failed to allocate memory for data_types");
        SET_ERR(err, errno, "Failed to allocate memory for columns data types", strerror(errno));
        return NULL;
    }

    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(filename, &error);
    if (reader == NULL)
    {
        report_g_error(error, err, "Failed to open file");
        free(data_types);
        return NULL;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if (schema == NULL)
    {
        report_g_error(error, err, "Failed to get schema");
        free(data_types);
        return NULL;
    }

    for (int i = 0; i < indices_count; i++)
    {
        GArrowField* field = garrow_schema_get_field(schema, indices[i]);
        if (field == NULL)
        {
            report_g_error(error, err, "Failed to get field");
            free(data_types);
            g_object_unref(field);
            g_object_unref(schema);
            g_object_unref(reader);
            return NULL;
        }

        GArrowDataType* data_type = garrow_field_get_data_type(field);

        gchar* data_type_string = garrow_data_type_to_string(data_type);
        LOG("Column %d has datatype %s\n", i, data_type_string);
        g_free(data_type_string);

        data_types[i] = worker_group_map_arrow_data_type(data_type, err);
        if (err->error_code != NO_ERROR)
        {
            free(data_types);
            g_object_unref(field);
            g_object_unref(schema);
            g_object_unref(reader);
            return NULL;
        }

        g_object_unref(field);
    }

    g_object_unref(schema);
    g_object_unref(reader);

    return data_types;
}

ColumnDataType worker_group_map_arrow_data_type(GArrowDataType* data_type, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return -1;
    }

    if (GARROW_IS_INT32_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_INT32;
    }

    if (GARROW_IS_INT64_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_INT64;
    }

    if (GARROW_IS_STRING_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_STRING;
    }

    LOG_INTERNAL_ERR("Unsupported data type");
    SET_ERR(err, INTERNAL_ERROR, "Unsupported data type", "");
    return COLUMN_DATA_TYPE_UNKNOWN;
}

void worker_group_calculate_new_column_indices(int* new_column_indices, const gint* old_column_indices,
                                         const int number_of_columns)
{
    gint* unique_values = malloc(sizeof(gint)*number_of_columns);
    int unique_count = 0;

    for (int i = 0; i < number_of_columns; i++)
    {
        int found = -1;
        for (int j = 0; j < unique_count; j++)
        {
            if (unique_values[j] == old_column_indices[i])
            {
                found = j;
                break;
            }
        }

        if (found == -1)
        {
            unique_values[unique_count] = old_column_indices[i];
            new_column_indices[i] = unique_count;
            unique_count++;
        }
        else
        {
            new_column_indices[i] = found;
        }
    }

    free(unique_values);
}