//
// Created by karol on 31.10.24.
//

#include "worker.h"

#include <stdio.h>
#include <parquet-glib/arrow-file-reader.h>
#include "hash_table.h"
#include "../../parquet_helpers/parquet_helpers.h"

void compute_file(const char* file,const ThreadData* data, HashTable* hash_table);
void print_thread_data(ThreadData* data);

void* compute_on_thread(void* arg) {
    ThreadData* data = (ThreadData*)arg;

    print_thread_data(data);

    HashTable* ht = create_hash_table(10);
    for(int i=0;i<data->n_files;i++) {
        compute_file(data->file_names[i], data, ht);
    }
    free(data);

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
        switch (data->selects[i].aggregate) {
            case MIN: agg_func = "MIN"; break;
            case MAX: agg_func = "MAX"; break;
            case AVG: agg_func = "AVG"; break;
            case MEDIAN: agg_func = "MEDIAN"; break;
            default: agg_func = "UNKNOWN"; break;
        }
        printf("Thread %d: Select Column %d: %d, Aggregate Function: %s\n",
                data->thread_index, i, data->selects[i].column_index, agg_func);
    }
}

void compute_file(const char* file, const ThreadData* data, HashTable* hash_table) {
    GError* error = NULL;
    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(file, &error);

    if(reader == NULL) {
        report_g_error(error);
        // TODO: handle failing thread computation
        return;
    }

    //TODO: actual computation of the aggregates
}

