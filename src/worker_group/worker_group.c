//
// Created by karol on 02.11.24.
//

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/sysinfo.h>
#include <string.h>
#include <unistd.h>
#include "../parquet_helpers/parquet_helpers.h"
#include "thread_data.h"
#include "worker_group.h"
#include "hash_table_interface.h"
#include "internal_to_proto_aggregate_converters.h"
#include "logging.h"
#include "workers/worker.h"

#define MAX_RAM_USAGE 0.7

// TODO() probably split this function
void worker_group_run_request(const QueryRequest* request, HashTable** request_hash_table,
                              HashTableInterface* hash_table_interface, ErrorInfo* err)
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
        worker_group_free_row_group_ranges(row_group_ranges, threads_count);
        return;
    }

    int* grouping_indices = (int*)malloc(sizeof(int) * request->n_group_columns);
    if (grouping_indices == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping indices");
        SET_ERR(err, errno, "Failed to allocate memory for grouping indices", strerror(errno));
        free(threads);
        worker_group_free_row_group_ranges(row_group_ranges, threads_count);
        return;
    }

    int* select_indices = (int*)malloc(sizeof(int) * request->n_select);
    if (select_indices == NULL)
    {
        LOG_ERR("Failed to allocate memory for select indices");
        SET_ERR(err, errno, "Failed to allocate memory for select indices", strerror(errno));
        free(threads);
        worker_group_free_row_group_ranges(row_group_ranges, threads_count);
        free(grouping_indices);
        return;
    }

    worker_group_get_columns_indices(request, grouping_indices, select_indices, err);
    if (err->error_code != NO_ERROR)
    {
        free(threads);
        worker_group_free_row_group_ranges(row_group_ranges, threads_count);
        free(grouping_indices);
        free(select_indices);
        return;
    }

    ColumnDataType* grouping_columns_data_type = worker_group_get_columns_data_types(grouping_indices,
        (int)request->n_group_columns, request->files_names[0], err);

    ColumnDataType* select_columns_data_type = worker_group_get_columns_data_types(select_indices,
        (int)request->n_select, request->files_names[0], err);
    if (err->error_code != NO_ERROR)
    {
        free(threads);
        for (int i = 0; i < threads_count; i++)
        {
            free(row_group_ranges[i]);
        }
        free(row_group_ranges);
        free(grouping_indices);
        free(select_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }

    int* hash_tables_max_size = worker_group_hash_tables_max_size(
        row_group_ranges, threads_count, request->n_files_names, err);

    if (err->error_code != NO_ERROR)
    {
        free(threads);
        for (int i = 0; i < threads_count; i++)
        {
            free(row_group_ranges[i]);
        }

        if (hash_tables_max_size != NULL)
        {
            free(hash_tables_max_size);
        }
        free(row_group_ranges);
        free(grouping_indices);
        free(select_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }

    ErrorInfo* thread_errors = (ErrorInfo*)malloc(sizeof(ErrorInfo) * threads_count);
    if (thread_errors == NULL)
    {
        LOG_ERR("Failed to allocate memory for thread data");
        SET_ERR(err, errno, "Failed to allocate memory for thread data", strerror(errno));
        free(threads);
        free(hash_tables_max_size);
        for (int i = 0; i < threads_count; i++)
        {
            free(row_group_ranges[i]);
        }
        free(row_group_ranges);
        free(grouping_indices);
        free(select_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }

    for (int i = 0; i < threads_count; i++)
    {
        memset(&thread_errors[i], 0, sizeof(thread_errors[i]));
    }

    ThreadData** thread_data = (ThreadData**)malloc(sizeof(ThreadData*) * threads_count);
    if (thread_data == NULL)
    {
        LOG_ERR("Failed to allocate memory for thread data");
        SET_ERR(err, errno, "Failed to allocate memory for thread data", strerror(errno));
        free(threads);
        free(thread_errors);
        free(hash_tables_max_size);
        worker_group_free_row_group_ranges(row_group_ranges, threads_count);
        free(grouping_indices);
        free(select_indices);
        free(grouping_columns_data_type);
        free(select_columns_data_type);
        return;
    }


    for (int i = 0; i < threads_count; i++)
    {
        thread_data[i] = worker_group_get_thread_data(request, i, threads_count, row_group_ranges[i],
                                                      grouping_indices, select_indices, grouping_columns_data_type,
                                                      select_columns_data_type, hash_table_interface,
                                                      hash_tables_max_size[i], thread_errors, err);
        if (err->error_code != NO_ERROR)
        {
            for (int j = 0; j < i; j++)
            {
                worker_group_free_thread_data(thread_data[j]);
            }
            worker_group_free_row_group_ranges(row_group_ranges, threads_count);
            free(thread_errors);
            free(hash_tables_max_size);
            free(threads);
            free(grouping_indices);
            free(select_indices);
            free(grouping_columns_data_type);
            free(select_columns_data_type);
            free(thread_data);
            return;
        }

        const int ret = pthread_create(&threads[i], NULL, worker_compute_on_thread, thread_data[i]);
        if (ret != 0)
        {
            LOG_ERR("Failed to create worker group thread");
            SET_ERR(err, ret, "Failed to create worker group thread", strerror(ret));
            for (int j = 0; j < i; j++)
            {
                worker_group_free_thread_data(thread_data[j]);
            }
            free(thread_errors);
            free(hash_tables_max_size);
            free(threads);
            free(row_group_ranges);
            free(grouping_indices);
            free(select_indices);
            free(grouping_columns_data_type);
            free(select_columns_data_type);
            free(thread_data);
            return;
        }
    }

    bool thread_failed = false;

    for (int i = 0; i < threads_count; i++)
    {
        void* result = NULL;
        const int ret = pthread_join(threads[i], &result);
        if (ret != 0)
        {
            LOG_ERR("Failed to join worker group thread");
            SET_ERR(err, ret, "Failed to join worker group thread", strerror(ret));
            thread_failed = true;
            // TODO handle exit?? or we need to wait for other thread anyway (now)
        }
        worker_group_free_thread_data(thread_data[i]);

        if (thread_errors[i].error_code != NO_ERROR)
        {
            LOG_ERR(thread_errors[i].error_message);
            SET_ERR(err, errno, thread_errors[i].error_message, strerror(ret));
            thread_failed = true;
            continue;
        }

        if (thread_failed)
        {
            continue;
        }

        HashTable* thread_ht = (HashTable*)result;

        if (*request_hash_table == NULL)
        {
            *request_hash_table = thread_ht;
        }
        else
        {
            hash_table_interface->combine_hash_tables(*request_hash_table, thread_ht, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_INTERNAL_ERR("Failed to run worker group: Failed to combine_hash_tables hash tables");
                SET_ERR(err, INTERNAL_ERROR, "Failed to run worker group", "Failed to combine_hash_tables hash tables");
                // TODO handle exit?? or we need to wait for other threads anyway (now)
            }
            hash_table_interface->free(thread_ht);
        }
    }

    worker_group_free_row_group_ranges(row_group_ranges, threads_count);
    free(thread_errors);
    free(hash_tables_max_size);
    free(threads);
    free(grouping_indices);
    free(select_indices);
    free(grouping_columns_data_type);
    free(select_columns_data_type);
    free(thread_data);
}

ThreadData* worker_group_get_thread_data(const QueryRequest* request, const int thread_index, const int num_threads,
                                         RowGroupsRange* row_groups_ranges, int* grouping_indices,
                                         const int* select_indices,
                                         ColumnDataType* group_columns_types, ColumnDataType* select_columns_types,
                                         HashTableInterface* ht_interface, int hash_tables_max_size,
                                         ErrorInfo* thread_errors, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    if (thread_errors == NULL)
    {
        LOG_INTERNAL_ERR("Passed errors array was NULL");
        SET_ERR(err, errno, "Passed errors array was NULL", "");
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

    thread_data->ht_interface = ht_interface;
    thread_data->ht_max_size = hash_tables_max_size;

    thread_data->thread_error = &thread_errors[thread_index];

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

    thread_data->group_columns_indices = grouping_indices;

    thread_data->selects_indices = (int*)malloc(sizeof(int) * thread_data->n_select);
    if (thread_data->selects_indices == NULL)
    {
        LOG_ERR("Failed to allocate memory for selects indices");
        SET_ERR(err, errno, "Failed to allocate memory for selects indices", strerror(errno));
        worker_group_free_thread_data(thread_data);
        return NULL;
    }

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
        thread_data->selects_indices[i] = select_indices[i];
        thread_data->selects_aggregate_functions[i] = convert_aggregate_function(select->function, err);
        if (err->error_code != NO_ERROR)
        {
            worker_group_free_thread_data(thread_data);
            return NULL;
        }
    }

    return thread_data;
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

    free(thread_data->selects_indices);
    free(thread_data->selects_aggregate_functions);
    free(thread_data);
}

RowGroupsRange** worker_group_get_row_group_ranges(const int n_files, char** file_names, const int num_threads,
                                                   ErrorInfo* err)
{
    if (n_files <= 0)
    {
        LOG_ERR("Invalid number of files");
        SET_ERR(err, errno, "Invalid number of files", "");
        return NULL;
    }

    if (num_threads <= 0)
    {
        LOG_ERR("Invalid number of threads: %d");
        SET_ERR(err, errno, "Invalid number of threads", "");
        return NULL;
    }

    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        SET_ERR(err, errno, "Passed error info was NULL", "");
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
        row_group_ranges[i] = NULL;
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
            worker_group_free_row_group_ranges(row_group_ranges, num_threads);

            return NULL;
        }

        const gint row_groups_count = gparquet_arrow_file_reader_get_n_row_groups(reader);
        if (row_groups_count < 0)
        {
            g_object_unref(reader);
            worker_group_free_row_group_ranges(row_group_ranges, num_threads);
            SET_ERR(err, INTERNAL_ERROR, "Failed to get row groups count", "");
            return NULL;
        }

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
            row_group_ranges[i] = NULL;
        }
    }

    free(row_group_ranges);
    row_group_ranges = NULL;
}

void worker_group_get_columns_indices(const QueryRequest* request, int* grouping_indices, int* select_indices,
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
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(request->files_names[0], &error);
    if (reader == NULL)
    {
        if (error != NULL)
        {
            report_g_error(error, err, "Failed to open file");
            g_error_free(error);
        }
        return;
    }

    GArrowSchema* schema = gparquet_arrow_file_reader_get_schema(reader, &error);
    if (schema == NULL)
    {
        if (error != NULL)
        {
            report_g_error(error, err, "Failed to get schema");
            g_error_free(error);
        }
        g_object_unref(reader);
        return;
    }

    for (int i = 0; i < request->n_group_columns; i++)
    {
        grouping_indices[i] = garrow_schema_get_field_index(schema, request->group_columns[i]);
        if (grouping_indices[i] == -1)
        {
            LOG_INTERNAL_ERR("Failed to get grouping indices: Cannot find provided column name");
            fprintf(stderr, "Cannot find provided column: %s\n", request->group_columns[i]);
            SET_ERR(err, INTERNAL_ERROR, "Failed to get grouping indices", "Cannot find provided column name");
            g_object_unref(schema);
            g_object_unref(reader);
            if (error != NULL)
            {
                g_error_free(error);
            }
            return;
        }
    }

    for (int i = 0; i < request->n_select; i++)
    {
        select_indices[i] = garrow_schema_get_field_index(schema, request->select[i]->column);
        if (select_indices[i] == -1)
        {
            LOG_INTERNAL_ERR("Failed to get select indices: Cannot find provided column name");
            fprintf(stderr, "Cannot find provided column: %s\n", request->select[i]->column);
            SET_ERR(err, INTERNAL_ERROR, "Failed to get select indices", "Cannot find provided column name");
            g_object_unref(schema);
            g_object_unref(reader);
            if (error != NULL)
            {
                g_error_free(error);
            }
            return;
        }
    }

    if (error != NULL)
    {
        g_error_free(error);
    }
    g_object_unref(schema);
    g_object_unref(reader);
}

ColumnDataType* worker_group_get_columns_data_types(const int* indices, int indices_count, const char* filename,
                                                    ErrorInfo* err)
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
            data_types = NULL;
            g_object_unref(field);
            g_object_unref(schema);
            g_object_unref(reader);
            return NULL;
        }

        GArrowDataType* data_type = garrow_field_get_data_type(field);
        if (data_type == NULL)
        {
            LOG_INTERNAL_ERR("Failed to get columns data types");
            SET_ERR(err, errno, "Failed to get columns data types", strerror(errno));
            free(data_types);
            data_types = NULL;
            g_object_unref(schema);
            g_object_unref(reader);
            g_object_unref(field);

            if (error != NULL)
            {
                g_error_free(error);
            }
            return NULL;
        }

        gchar* data_type_string = garrow_data_type_to_string(data_type);
        if (data_type_string == NULL)
        {
            LOG_INTERNAL_ERR("Failed to get columns data types");
            SET_ERR(err, INTERNAL_ERROR, "Failed to get columns data types", strerror(errno));

            free(data_types);
            g_object_unref(schema);
            g_object_unref(reader);
            g_object_unref(field);

            if (error != NULL)
            {
                g_error_free(error);
            }
            return NULL;
        }

        LOG("Column %d has datatype %s\n", i, data_type_string);
        g_free(data_type_string);

        data_types[i] = worker_group_map_arrow_data_type(data_type, err);
        if (err->error_code != NO_ERROR)
        {
            LOG_INTERNAL_ERR("Failed to get columns data types");
            SET_ERR(err, INTERNAL_ERROR, "Failed to get columns data types", strerror(errno));
            free(data_types);
            g_object_unref(schema);
            g_object_unref(reader);
            if (error != NULL)
            {
                g_error_free(error);
            }
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

    if (data_type == NULL)
    {
        LOG_INTERNAL_ERR("Passed data_type was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Passed data_type was NULL", "");
        return COLUMN_DATA_TYPE_UNKNOWN;
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

    if (GARROW_IS_FLOAT_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_FLOAT;
    }

    if (GARROW_IS_DOUBLE_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_DOUBLE;
    }

    if (GARROW_IS_BOOLEAN_DATA_TYPE(data_type))
    {
        return COLUMN_DATA_TYPE_BOOLEAN;
    }

    LOG_INTERNAL_ERR("Unknown data type");
    SET_ERR(err, INTERNAL_ERROR, "Unknown data type", "");
    return COLUMN_DATA_TYPE_UNKNOWN;
}


unsigned long int get_available_ram()
{
    struct sysinfo info;
    if (sysinfo(&info) != 0)
    {
        perror("sysinfo failed");
        return -1;
    }
    return info.freeram;
}

int* worker_group_hash_tables_max_size(RowGroupsRange** row_group_ranges,
                                       const int num_threads, const int num_files, ErrorInfo* err)
{
    if (row_group_ranges == NULL)
    {
        LOG_INTERNAL_ERR("Passed row_group_ranges was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Passed row_group_ranges was NULL", "");
        return NULL;
    }
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Passed error info was NULL", "");
        return NULL;
    }

    int* thread_entries_limits = (int*)malloc(sizeof(int) * num_threads);

    if (thread_entries_limits == NULL)
    {
        SET_ERR(err, errno, "Failed to allocate memory for thread_entries_limits", strerror(errno));
        LOG_ERR("Failed to allocate memory for thread_entries_limits");
        return NULL;
    }

    int* row_group_count_per_thread = (int*)malloc(sizeof(int) * num_threads);
    if (row_group_count_per_thread == NULL)
    {
        SET_ERR(err, errno, "Failed to allocate memory for row_group_count_per_thread", strerror(errno));
        LOG_ERR("Failed to allocate memory for row_group_count_per_thread");
        free(thread_entries_limits);
        return NULL;
    }

    for (int i = 0; i < num_threads; i++)
    {
        row_group_count_per_thread[i] = 0;
    }

    int total_row_groups = 0;
    for (int i = 0; i < num_threads; i++)
    {
        for (int j = 0; j < num_files; j++)
        {
            total_row_groups += row_group_ranges[i][j].count;
            row_group_count_per_thread[i] += row_group_ranges[i][j].count;
        }
    }

    if (total_row_groups == 0)
    {
        SET_ERR(err, INTERNAL_ERROR, "Total row groups is 0", "");
        LOG_ERR("Total row groups is 0");

        free(thread_entries_limits);
        free(row_group_count_per_thread);
        return NULL;
    }

    unsigned long int available_ram = get_available_ram();
    unsigned long int usable_ram = (unsigned long int)available_ram * MAX_RAM_USAGE;

    int hash_table_entry_size = sizeof(HashTableEntry);
    int hash_table_value_size = sizeof(HashTableValue);
    int avg_entry_size = hash_table_entry_size + hash_table_value_size * 3;

    unsigned long int total_usable_entries = usable_ram / avg_entry_size;
    unsigned long entries_per_row_group = total_usable_entries / total_row_groups;
    for (int i = 0; i < num_threads; i++)
    {
        thread_entries_limits[i] = (int)entries_per_row_group * row_group_count_per_thread[i];
    }

    free(row_group_count_per_thread);
    return thread_entries_limits;
}
