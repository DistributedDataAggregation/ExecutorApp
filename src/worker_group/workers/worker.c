//
// Created by karol on 31.10.24.
//

#include "worker.h"

#include <stdio.h>
#include <parquet-glib/arrow-file-reader.h>

#include "error_utilites.h"
#include "hash_table.h"
#include "../../parquet_helpers/parquet_helpers.h"

#include <unistd.h>
#include <sys/types.h>


void compute_file(int index_of_the_file,const ThreadData* data, HashTable* hash_table);
void print_thread_data(ThreadData* data);
char* get_grouping_string(GArrowArray* grouping_array, ColumnDataType data_type, int row_index) ;
char* construct_grouping_string(int n_group_columns, GArrowArray** grouping_arrays, int row_index, ColumnDataType* group_columns_data_types);
HashTableValue get_hash_table_value(
    GArrowArray* select_array,
    int row_index,
    ColumnDataType select_columns_data_types,
    AggregateFunction aggregate_function);
HashTableValue update_value(HashTableValue current_value, HashTableValue incoming_value);

void* compute_on_thread(void* arg) {
    ThreadData* data = (ThreadData*)arg;

    print_thread_data(data);

    HashTable* ht = create_hash_table(10);
    for(int i=0;i<data->n_files;i++) {
        compute_file(i, data, ht);
        printf("[%d] Finished file: %s\n", data->thread_index, data->file_names[i]);
    }

    printf("[%d] Finished\n", data->thread_index);

    return ht;
}

void print_thread_data(ThreadData* data) {
    if (!data) return;

    // Preface every print with thread_index
    printf("Thread %d:\n", data->thread_index);

    // Print number of threads
    printf("Thread %d: Number of threads: %d\n", data->thread_index, data->num_threads);

    // Print number of files and file names
    printf("Thread %d: Number of files: %d\n", data->thread_index, data->n_files);
    for (int i = 0; i < data->n_files; i++) {
        printf("Thread %d: File %d: %s\n", data->thread_index, i, data->file_names[i]);
        printf("Thread %d: File %d Row Groups - Start: %d, Count: %d\n",
                data->thread_index, i, data->file_row_groups_ranges[i].start,
                data->file_row_groups_ranges[i].count);
    }

    // Print group columns
    printf("Thread %d: Number of group columns: %d\n", data->thread_index, data->n_group_columns);
    for (int i = 0; i < data->n_group_columns; i++) {
        printf("Thread %d: Group Column %d: %d\n", data->thread_index, i, data->group_columns_indices[i]);
    }

    // Print selected columns and aggregate functions
    printf("Thread %d: Number of select columns: %d\n", data->thread_index, data->n_select);
    for (int i = 0; i < data->n_select; i++) {
        char* agg_func;
        switch (data->selects_aggregate_functions[i]) {
            case MIN: agg_func = "MIN"; break;
            case MAX: agg_func = "MAX"; break;
            case AVG: agg_func = "AVG"; break;
            case MEDIAN: agg_func = "MEDIAN"; break;
            default: agg_func = "UNKNOWN"; break;
        }
        printf("Thread %d: Select Column %d: %d, Aggregate Function: %s\n",
                data->thread_index, i, data->selects_indices[i], agg_func);
    }
}

const char* column_data_type_to_string(ColumnDataType type) {
    switch (type) {
        case COLUMN_DATA_TYPE_INT32:
            return "INT32";
        case COLUMN_DATA_TYPE_INT64:
            return "INT64";
        case COLUMN_DATA_TYPE_STRING:
            return "STRING";
        case COLUMN_DATA_TYPE_UNKNOWN:
            default:
                return "UNKNOWN";
    }
}


// TODO: refactor this method
// split up the code into multiple functions
// f.e. i have made lots of mistakes with loop indexing where in nested loops i have used the outer loop iterator
// instead of the inside one f.e using variable i instead of j
// renaming would probably be enough in that case


void compute_file(int index_of_the_file,const ThreadData* data, HashTable* hash_table){
    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(data->file_names[index_of_the_file], &error);

    if(reader == NULL) {
        report_g_error(error);
        // TODO: handle failing thread computation
        return;
    }

    // TODO: actual computation of the aggregates
    int start_row_group = data->file_row_groups_ranges[index_of_the_file].start;
    int count_row_groups = data->file_row_groups_ranges[index_of_the_file].count;
    int end = start_row_group + count_row_groups;


    int number_of_columns = (data->n_group_columns+ data->n_select);

    gint* columns_indices = malloc(sizeof(gint) * number_of_columns);
    if(columns_indices == NULL) {
        report_g_error(error);
        return;
    }

    for(int i = 0;i<data->n_group_columns;i++) {
        columns_indices[i] = data->group_columns_indices[i];
    }

    for(int i=data->n_group_columns; i < number_of_columns; i++) {
        columns_indices[i] = data->selects_indices[i-data->n_group_columns];
    }

    for(int i=0;i<number_of_columns;i++) {
        printf("[%d] %dth column has index %d\n", data->thread_index, i, columns_indices[i]);
    }

    for(int i = start_row_group; i < end; i++) {
        GArrowTable* table = gparquet_arrow_file_reader_read_row_group(reader, i, columns_indices, number_of_columns, &error);
        if(table == NULL) {
            report_g_error(error);
            return;
        }

        GArrowChunkedArray** grouping_chunked_arrays = malloc(sizeof(GArrowChunkedArray*) * data->n_group_columns);
        GArrowChunkedArray** select_chunked_arrays = malloc(sizeof(GArrowChunkedArray*) * data->n_select);

        for(int j = 0; j < data->n_group_columns; j++) {
            grouping_chunked_arrays[j] = garrow_table_get_column_data(table, j);
            if(grouping_chunked_arrays[j] == NULL) {
                INTERNAL_ERROR("Can't find grouping_chunked_arrays");
            }
        }

        for(int j=0; j < data->n_select; j++) {
            select_chunked_arrays[j] = garrow_table_get_column_data(table, j + data->n_group_columns);
            if(select_chunked_arrays[j] == NULL) {
                INTERNAL_ERROR("Can't find select_chunked_arrays");
            }
        }

        const gint n_chunks = garrow_chunked_array_get_n_chunks(grouping_chunked_arrays[0]);
        for(int chunk_index = 0; chunk_index< n_chunks; chunk_index++) {

            GArrowArray** grouping_arrays = malloc(sizeof(GArrowArray*) * data->n_group_columns);
            if(grouping_arrays == NULL) {
                //TODO: free allocated data
                return;
            }
            for(int grouping = 0; grouping < data->n_group_columns; grouping++) {
                grouping_arrays[grouping] = garrow_chunked_array_get_chunk(grouping_chunked_arrays[grouping], chunk_index);
            }

            GArrowArray** select_arrays = malloc(sizeof(GArrowArray*) * data->n_select);
            if(select_arrays == NULL) {
                // TODO: free allocated data
                return;
            }

            for(int select = 0; select < data->n_select; select++) {
                select_arrays[select] = garrow_chunked_array_get_chunk(select_chunked_arrays[select], chunk_index);
            }

            gint number_of_rows = garrow_array_count(grouping_arrays[0], NULL, &error);
            if(error != NULL) {
                report_g_error(error);
                g_object_unref(select_arrays);
                free(grouping_arrays);
                return;
            }

            for(int row_index = 0; row_index < number_of_rows; row_index++) {
                int grouping_string_size = 0;
                char* grouping_string = construct_grouping_string(data->n_group_columns, grouping_arrays, row_index, data->group_columns_data_types);

                //printf("Grouping string: %s\n", grouping_string);

                // TODO:
                //calculate the aggregates
                HashTableValue* hash_table_values = malloc(sizeof(HashTableValue)*data->n_select);
                if(hash_table_values == NULL) {
                    REPORT_ERR("malloc");
                    // TODO: memory deallocation
                    return;
                }

                for(int select_index=0; select_index<data->n_select; select_index++) {
                    hash_table_values[select_index] = get_hash_table_value(
                        select_arrays[select_index],
                        row_index,
                        data->select_columns_types[select_index],
                        data->selects_aggregate_functions[select_index]);


                }

                HashTableEntry* found = search(hash_table, grouping_string);
                if(found == NULL) {
                    // add grouping into hash table
                    HashTableEntry* new_entry = malloc(sizeof(HashTableEntry));
                    if(new_entry == NULL) {
                        REPORT_ERR("malloc");
                        return;
                    }

                    new_entry->key = grouping_string;
                    new_entry->values = hash_table_values;
                    new_entry->n_values = data->n_select;
                    new_entry->next = NULL;
                    insert(hash_table, new_entry);
                } else {
                    for(int value_index = 0; value_index < data->n_select; value_index++) {
                        found->values[value_index] = update_value(
                            found->values[value_index],
                            hash_table_values[value_index]);
                    }
                    free(hash_table_values);
                    free(grouping_string);
                }
               // printf("[%d] processed row index %d\n", data->thread_index, row_index);
            }


            for(int grouping = 0; grouping < data->n_group_columns; grouping++) {
                g_object_unref(grouping_arrays[grouping]);
            }

            for(int select = 0; select < data->n_select; select++) {
                g_object_unref(select_arrays[select]);
            }


            free(select_arrays);
            free(grouping_arrays);
        }

        for(int grouping_index=0; grouping_index < data->n_group_columns; grouping_index++) {
            g_object_unref(grouping_chunked_arrays[grouping_index]);
        }

        for(int select_index=0; select_index < data->n_select; select_index++) {
            g_object_unref(select_chunked_arrays[select_index]);
        }

        free(grouping_chunked_arrays);
        free(select_chunked_arrays);
        g_object_unref(table);

        //printf("[%d] Finished row group number %d\n", data->thread_index, i);
    }

    //printf("[%d] Finished calculations for file\n", data->thread_index);
    g_object_unref(reader);
    free(columns_indices);
}

char* get_grouping_string(GArrowArray* grouping_array, ColumnDataType data_type, int row_index) {
    switch (data_type) {
        case COLUMN_DATA_TYPE_INT32:
            GArrowInt32Array* int32_array = GARROW_INT32_ARRAY(grouping_array);
            int int32_value= garrow_int32_array_get_value(int32_array, row_index);
            return g_strdup_printf("%d", int32_value);
        case COLUMN_DATA_TYPE_INT64:
            GArrowInt64Array* int64_array = GARROW_INT64_ARRAY(grouping_array);
            long int64_value = garrow_int64_array_get_value(int64_array, row_index);
            return g_strdup_printf("%ld", int64_value);
        case COLUMN_DATA_TYPE_STRING:
            GArrowStringArray* string_array = GARROW_STRING_ARRAY(grouping_array);
            return garrow_string_array_get_string(string_array, row_index);
        case COLUMN_DATA_TYPE_UNKNOWN:
        default:
            return NULL;
    }
}

char* construct_grouping_string(int n_group_columns, GArrowArray** grouping_arrays, int row_index, ColumnDataType* group_columns_data_types) {
    char* grouping_string = malloc(1);
    grouping_string[0] = '\0';

    int grouping_string_size = 1;
    for(int grouping_col_index = 0; grouping_col_index < n_group_columns; grouping_col_index++) {
        char* column_value_string = get_grouping_string(
            grouping_arrays[grouping_col_index],
            group_columns_data_types[grouping_col_index],
            row_index);

        int current_length = strlen(column_value_string);
        grouping_string = (char*)realloc(grouping_string, grouping_string_size + current_length + 1);
        memset(grouping_string+grouping_string_size - 1, 0, current_length);
        grouping_string_size += current_length;
        grouping_string = strcat(grouping_string, column_value_string);
        grouping_string[grouping_string_size - 1] = '\0';
        free(column_value_string);
    }

    return grouping_string;
}

HashTableValue get_hash_table_value(
    GArrowArray* select_array,
    int row_index,
    ColumnDataType select_columns_data_types,
    AggregateFunction aggregate_function){

    HashTableValue hash_table_value;
    hash_table_value.aggregate_function = UNKNOWN;

    switch(aggregate_function) {
        case MIN: {
            hash_table_value.aggregate_function = MIN;
            break;
        }
        case MAX: {
            hash_table_value.aggregate_function = MAX;
            break;
        }
        case AVG: {
            hash_table_value.aggregate_function = AVG;
            break;
        }
        case MEDIAN:{
            hash_table_value.aggregate_function = MEDIAN;
            break;
        }
        case UNKNOWN: {
            hash_table_value.aggregate_function = UNKNOWN;
            break;
        }
    }

    long value = 0;
    switch (select_columns_data_types) {
        case COLUMN_DATA_TYPE_INT32:
            GArrowInt32Array* int32_array = GARROW_INT32_ARRAY(select_array);
            value= garrow_int32_array_get_value(int32_array, row_index);
            break;
        case COLUMN_DATA_TYPE_INT64:
            GArrowInt64Array* int64_array = GARROW_INT64_ARRAY(select_array);
            value = garrow_int64_array_get_value(int64_array, row_index);
            break;
        case COLUMN_DATA_TYPE_STRING:
        case COLUMN_DATA_TYPE_UNKNOWN:
            fprintf(stderr, "Wrong column type for aggregation. Only integer types are allowed!\n");
    }

    if(hash_table_value.aggregate_function == AVG) {
        hash_table_value.accumulator = value;
        hash_table_value.count = 1;
    }
    else {
        hash_table_value.value = value;
    }

    return hash_table_value;
}

