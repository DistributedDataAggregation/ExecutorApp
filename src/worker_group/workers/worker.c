//
// Created by karol on 31.10.24.
//

#include <parquet-glib/arrow-file-reader.h>
#include <stdio.h>
#include "../../parquet_helpers/parquet_helpers.h"
#include "hash_table.h"
#include "worker.h"
#include <sys/types.h>

#include "logging.h"

void* compute_on_thread(void* arg)
{
    ThreadData* data = (ThreadData*)arg;
    ErrorInfo err = {0};
    // TODO change that to array of errorinfo for each thread that will be checked after join by main thread
    print_thread_data(data);

    HashTable* ht = hash_table_create(10, &err);
    for (int i = 0; i < data->n_files; i++)
    {
        compute_file(i, data, ht, &err);
        LOG("[%d] Finished file: %s\n", data->thread_index, data->file_names[i]);
    }

    LOG("[%d] Finished\n", data->thread_index);

    return ht;
}

void print_thread_data(ThreadData* data)
{
    if (!data) return;

    // Preface every print with thread_index
    LOG("Thread %d:\n", data->thread_index);

    // Print number of threads
    LOG("Thread %d: Number of threads: %d\n", data->thread_index, data->num_threads);

    // Print number of files and file names
    LOG("Thread %d: Number of files: %d\n", data->thread_index, data->n_files);
    for (int i = 0; i < data->n_files; i++)
    {
        LOG("Thread %d: File %d: %s\n", data->thread_index, i, data->file_names[i]);
        LOG("Thread %d: File %d Row Groups - Start: %d, Count: %d\n",
            data->thread_index, i, data->file_row_groups_ranges[i].start,
            data->file_row_groups_ranges[i].count);
    }

    // Print group columns
    LOG("Thread %d: Number of group columns: %d\n", data->thread_index, data->n_group_columns);
    for (int i = 0; i < data->n_group_columns; i++)
    {
        LOG("Thread %d: Group Column %d: %d\n", data->thread_index, i, data->group_columns_indices[i]);
    }

    // Print selected columns and aggregate functions
    LOG("Thread %d: Number of select columns: %d\n", data->thread_index, data->n_select);
    for (int i = 0; i < data->n_select; i++)
    {
        char* agg_func;
        switch (data->selects_aggregate_functions[i])
        {
        case MIN: agg_func = "MIN";
            break;
        case MAX: agg_func = "MAX";
            break;
        case AVG: agg_func = "AVG";
            break;
        case MEDIAN: agg_func = "MEDIAN";
            break;
        default: agg_func = "UNKNOWN";
            break;
        }
        LOG("Thread %d: Select Column %d: %d, Aggregate Function: %s\n",
            data->thread_index, i, data->selects_indices[i], agg_func);
    }
}


// TODO: refactor this method
// split up the code into multiple functions
// f.e. i have made lots of mistakes with loop indexing where in nested loops i have used the outer loop iterator
// instead of the inside one f.e using variable i instead of j
// renaming would probably be enough in that case

void compute_file(const int index_of_the_file, const ThreadData* data, HashTable* hash_table, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_THREAD_ERR("Passed error info was NULL", data->thread_index);
        return;
    }

    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(data->file_names[index_of_the_file], &error);
    if (reader == NULL)
    {
        report_g_error(error, err, "Failed to open file");
        // TODO: handle failing thread computation
        return;
    }

    // TODO: actual computation of the aggregates
    const int start_row_group = data->file_row_groups_ranges[index_of_the_file].start;
    const int count_row_groups = data->file_row_groups_ranges[index_of_the_file].count;
    const int end = start_row_group + count_row_groups;

    const int number_of_columns = (data->n_group_columns + data->n_select);

    gint* columns_indices = malloc(sizeof(gint) * number_of_columns);
    if (columns_indices == NULL)
    {
        report_g_error(error, err, "Failed to allocate memory for columns indices");
        return;
    }

    for (int i = 0; i < data->n_group_columns; i++)
    {
        columns_indices[i] = data->group_columns_indices[i];
    }

    for (int i = data->n_group_columns; i < number_of_columns; i++)
    {
        columns_indices[i] = data->selects_indices[i - data->n_group_columns];
    }

    int* new_columns_indices = malloc(sizeof(int) * number_of_columns);
    if (!new_columns_indices)
    {
        report_g_error(error, err, "Failed to allocate memory for new columns indices");
        return;
    }
    worker_calculate_new_column_indices(new_columns_indices, columns_indices, number_of_columns);

    for (int i = 0; i < number_of_columns; i++)
    {
        LOG("[%d] %dth column has index %d\n", data->thread_index, i, columns_indices[i]);
    }

    for (int i = start_row_group; i < end; i++)
    {
        GArrowTable* table = gparquet_arrow_file_reader_read_row_group(reader, i, columns_indices, number_of_columns,
                                                                       &error);
        if (table == NULL)
        {
            report_g_error(error, err, "Failed to read row group");
            g_object_unref(reader);
            free(columns_indices);
            free(new_columns_indices);
            return;
        }

        GArrowChunkedArray** grouping_chunked_arrays = malloc(sizeof(GArrowChunkedArray*) * data->n_group_columns);
        if (grouping_chunked_arrays == NULL)
        {
            LOG_THREAD_ERR("Failed to allocate memory for grouping chunked arrays", data->thread_index);
            SET_ERR(err, errno, "Failed to allocate memory for grouping chunked arrays", strerror(errno));
            g_object_unref(reader);
            free(columns_indices);
            free(new_columns_indices);
            g_object_unref(table);
            return;
        }

        GArrowChunkedArray** select_chunked_arrays = malloc(sizeof(GArrowChunkedArray*) * data->n_select);
        if (select_chunked_arrays == NULL)
        {
            LOG_THREAD_ERR("Failed to allocate memory for select chunked arrays", data->thread_index);
            SET_ERR(err, errno, "Failed to allocate memory for select chunked arrays", strerror(errno));
            g_object_unref(reader);
            free(columns_indices);
            free(new_columns_indices);
            g_object_unref(table);
            free(grouping_chunked_arrays);
            return;
        }

        for (int j = 0; j < data->n_group_columns; j++)
        {
            grouping_chunked_arrays[j] = garrow_table_get_column_data(table, new_columns_indices[j]);
            if (grouping_chunked_arrays[j] == NULL)
            {
                LOG_INTERNAL_THREAD_ERR("Failed to find grouping chunked arrays", data->thread_index);
                SET_ERR(err, INTERNAL_ERROR, "Failed to find grouping chunked arrays", "");
                g_object_unref(reader);
                free(columns_indices);
                free(new_columns_indices);
                g_object_unref(table);
                for (int grouping_index = 0; grouping_index < j; grouping_index++)
                {
                    g_object_unref(grouping_chunked_arrays[grouping_index]);
                }
                free(grouping_chunked_arrays);
                free(select_chunked_arrays);
                return;
            }
        }

        for (int j = 0; j < data->n_select; j++)
        {
            select_chunked_arrays[j] = garrow_table_get_column_data(
                table, new_columns_indices[j + data->n_group_columns]);
            if (select_chunked_arrays[j] == NULL)
            {
                LOG_INTERNAL_THREAD_ERR("Failed to find select chunked arrays", data->thread_index);
                SET_ERR(err, INTERNAL_ERROR, "Failed to find select chunked arrays", "");
                g_object_unref(reader);
                free(columns_indices);
                free(new_columns_indices);
                g_object_unref(table);
                for (int grouping_index = 0; grouping_index < data->n_group_columns; grouping_index++)
                {
                    g_object_unref(grouping_chunked_arrays[grouping_index]);
                }
                for (int select_index = 0; select_index < j; select_index++)
                {
                    g_object_unref(select_chunked_arrays[select_index]);
                }
                free(grouping_chunked_arrays);
                free(select_chunked_arrays);
                return;
            }
        }

        const gint n_chunks = (gint)garrow_chunked_array_get_n_chunks(grouping_chunked_arrays[0]);
        for (int chunk_index = 0; chunk_index < n_chunks; chunk_index++)
        {
            GArrowArray** grouping_arrays = malloc(sizeof(GArrowArray*) * data->n_group_columns);
            if (grouping_arrays == NULL)
            {
                LOG_THREAD_ERR("Failed to allocate memory for grouping arrays", data->thread_index);
                SET_ERR(err, errno, "Failed to allocate memory for grouping arrays", strerror(errno));
                g_object_unref(reader);
                free(columns_indices);
                free(new_columns_indices);
                g_object_unref(table);
                for (int grouping_index = 0; grouping_index < data->n_group_columns; grouping_index++)
                {
                    g_object_unref(grouping_chunked_arrays[grouping_index]);
                }
                for (int select_index = 0; select_index < data->n_select; select_index++)
                {
                    g_object_unref(select_chunked_arrays[select_index]);
                }
                free(grouping_chunked_arrays);
                free(select_chunked_arrays);
                return;
            }

            for (int grouping = 0; grouping < data->n_group_columns; grouping++)
            {
                grouping_arrays[grouping] = garrow_chunked_array_get_chunk(
                    grouping_chunked_arrays[grouping], chunk_index);
            }

            GArrowArray** select_arrays = malloc(sizeof(GArrowArray*) * data->n_select);
            if (select_arrays == NULL)
            {
                // TODO free allocated data
                return;
            }

            for (int select = 0; select < data->n_select; select++)
            {
                select_arrays[select] = garrow_chunked_array_get_chunk(select_chunked_arrays[select], chunk_index);
            }

            const guint number_of_rows = garrow_chunked_array_get_n_rows(grouping_chunked_arrays[chunk_index]);
            if (error != NULL)
            {
                report_g_error(error, err, "Failed to retrieve number of rows");
                // TODO free allocated data
                return;
            }

            for (int row_index = 0; row_index < number_of_rows; row_index++)
            {
                char* grouping_string = construct_grouping_string(data->n_group_columns, grouping_arrays, row_index,
                                                                  data->group_columns_data_types, err);
                if (err->error_code != NO_ERROR)
                {
                    // TODO free allocated data
                    return;
                }

                //LOG("Grouping string: %s\n", grouping_string);

                // TODO:
                //calculate the aggregates
                HashTableValue* hash_table_values = malloc(sizeof(HashTableValue) * data->n_select);
                if (hash_table_values == NULL)
                {
                    LOG_THREAD_ERR("Failed to allocate memory for hash table values", data->thread_index);
                    SET_ERR(err, errno, "Failed to allocate memory for hash table values", strerror(errno));
                    // TODO free allocated data
                    return;
                }

                for (int select_index = 0; select_index < data->n_select; select_index++)
                {
                    hash_table_values[select_index] = get_hash_table_value(select_arrays[select_index],
                                                                           row_index,
                                                                           data->select_columns_types[select_index],
                                                                           data->selects_aggregate_functions[
                                                                               select_index], err);
                    if (err->error_code != NO_ERROR)
                    {
                        // TODO free allocated data
                        return;
                    }
                }

                const HashTableEntry* found = hash_table_search(hash_table, grouping_string);
                if (found == NULL)
                {
                    // add grouping into hash table
                    HashTableEntry* new_entry = malloc(sizeof(HashTableEntry));
                    if (new_entry == NULL)
                    {
                        LOG_THREAD_ERR("Failed to allocate memory for hash table entry", data->thread_index);
                        SET_ERR(err, errno, "Failed to allocate memory for hash table entry", strerror(errno));
                        // TODO free allocated data
                        return;
                    }

                    new_entry->key = grouping_string;
                    new_entry->values = hash_table_values;
                    new_entry->n_values = data->n_select;
                    new_entry->next = NULL;
                    hash_table_insert(hash_table, new_entry, err);
                    if (err->error_code != NO_ERROR)
                    {
                        // TODO free allocated data
                        return;
                    }
                }
                else
                {
                    for (int value_index = 0; value_index < data->n_select; value_index++)
                    {
                        found->values[value_index] = hash_table_update_value(
                            found->values[value_index], hash_table_values[value_index], err);
                        if (err->error_code != NO_ERROR)
                        {
                            // TODO free allocated data
                            return;
                        }
                    }
                    free(hash_table_values);
                    free(grouping_string);
                }
                // LOG("[%d] processed row index %d\n", data->thread_index, row_index);
            }


            for (int grouping = 0; grouping < data->n_group_columns; grouping++)
            {
                g_object_unref(grouping_arrays[grouping]);
            }

            for (int select = 0; select < data->n_select; select++)
            {
                g_object_unref(select_arrays[select]);
            }
        }

        for (int grouping_index = 0; grouping_index < data->n_group_columns; grouping_index++)
        {
            g_object_unref(grouping_chunked_arrays[grouping_index]);
        }

        for (int select_index = 0; select_index < data->n_select; select_index++)
        {
            g_object_unref(select_chunked_arrays[select_index]);
        }

        g_object_unref(table);
        free(grouping_chunked_arrays);
        free(select_chunked_arrays);

        //LOG("[%d] Finished row group number %d\n", data->thread_index, i);
    }

    //LOG("[%d] Finished calculations for file\n", data->thread_index);
    g_object_unref(reader);
    free(columns_indices);
    free(new_columns_indices);
}

char* get_grouping_string(GArrowArray* grouping_array, const ColumnDataType data_type, const int row_index,
                          ErrorInfo* err)
{
    switch (data_type)
    {
    case COLUMN_DATA_TYPE_INT32:
        GArrowInt32Array* int32_array = GARROW_INT32_ARRAY(grouping_array);
        const int int32_value = garrow_int32_array_get_value(int32_array, row_index);
        return g_strdup_printf("%d", int32_value);
    case COLUMN_DATA_TYPE_INT64:
        GArrowInt64Array* int64_array = GARROW_INT64_ARRAY(grouping_array);
        const long int64_value = garrow_int64_array_get_value(int64_array, row_index);
        return g_strdup_printf("%ld", int64_value);
    case COLUMN_DATA_TYPE_STRING:
        GArrowStringArray* string_array = GARROW_STRING_ARRAY(grouping_array);
        return garrow_string_array_get_string(string_array, row_index);
    case COLUMN_DATA_TYPE_UNKNOWN:
    default:
        LOG_INTERNAL_ERR("Unsupported data type");
        SET_ERR(err, INTERNAL_ERROR, "Unsupported data type", "");
        return NULL;
    }
}

char* construct_grouping_string(const int n_group_columns, GArrowArray** grouping_arrays, const int row_index,
                                const ColumnDataType* group_columns_data_types, ErrorInfo* err)
{
    char* grouping_string = malloc(1);
    if (grouping_string == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping string");
        SET_ERR(err, errno, "Failed to allocate memory for grouping string", strerror(errno));
        return NULL;
    }

    grouping_string[0] = '\0';

    int grouping_string_size = 1;
    for (int grouping_col_index = 0; grouping_col_index < n_group_columns; grouping_col_index++)
    {
        char* column_value_string = get_grouping_string(
            grouping_arrays[grouping_col_index],
            group_columns_data_types[grouping_col_index],
            row_index, err);
        if (err->error_code != NO_ERROR)
        {
            free(grouping_string);
            free(column_value_string);
            return NULL;
        }

        const int current_length = (int)strlen(column_value_string);
        grouping_string = (char*)realloc(grouping_string, grouping_string_size + current_length + 1);
        if (grouping_string == NULL)
        {
            LOG_ERR("Failed to allocate memory for grouping string");
            SET_ERR(err, errno, "Failed to allocate memory for grouping string", strerror(errno));
            free(column_value_string);
            return NULL;
        }

        memset(grouping_string + grouping_string_size - 1, 0, current_length);
        grouping_string_size += current_length;
        grouping_string = strcat(grouping_string, column_value_string);
        free(column_value_string);
        if (grouping_string == NULL)
        {
            LOG_ERR("Failed to concatenation grouping string");
            SET_ERR(err, INTERNAL_ERROR, "Failed to concatenation grouping string", "");
            return NULL;
        }
        grouping_string[grouping_string_size - 1] = '\0';
    }

    return grouping_string;
}

HashTableValue get_hash_table_value(GArrowArray* select_array, const int row_index,
                                    const ColumnDataType select_columns_data_types,
                                    const AggregateFunction aggregate_function, ErrorInfo* err)
{
    HashTableValue hash_table_value = {0};
    hash_table_value.aggregate_function = UNKNOWN;

    switch (aggregate_function)
    {
    case MIN:
        {
            hash_table_value.aggregate_function = MIN;
            break;
        }
    case MAX:
        {
            hash_table_value.aggregate_function = MAX;
            break;
        }
    case AVG:
        {
            hash_table_value.aggregate_function = AVG;
            break;
        }
    case MEDIAN:
        {
            hash_table_value.aggregate_function = MEDIAN;
            break;
        }
    case UNKNOWN:
        {
            hash_table_value.aggregate_function = UNKNOWN;
            break;
        }
    }

    long value = 0;
    switch (select_columns_data_types)
    {
    case COLUMN_DATA_TYPE_INT32:
        GArrowInt32Array* int32_array = GARROW_INT32_ARRAY(select_array);
        value = garrow_int32_array_get_value(int32_array, row_index);
        break;
    case COLUMN_DATA_TYPE_INT64:
        GArrowInt64Array* int64_array = GARROW_INT64_ARRAY(select_array);
        value = garrow_int64_array_get_value(int64_array, row_index);
        break;
    case COLUMN_DATA_TYPE_STRING:
    case COLUMN_DATA_TYPE_UNKNOWN:
        LOG_INTERNAL_ERR("Unsupported data type for aggregation. Only integer types are allowed");
        SET_ERR(err, INTERNAL_ERROR, "Unsupported data type", "");
    }

    if (hash_table_value.aggregate_function == AVG)
    {
        hash_table_value.value = value;
        hash_table_value.count = 1;
    }
    else
    {
        hash_table_value.value = value;
    }

    return hash_table_value;
}

// TODO () move this function before thread?
void worker_calculate_new_column_indices(int* new_column_indices, const gint* old_column_indices,
                                         const int number_of_columns)
{
    // change it
    gint unique_values[number_of_columns];
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
}
