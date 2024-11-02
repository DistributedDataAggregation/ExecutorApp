//
// Created by karol on 31.10.24.
//

#include "worker.h"
#include <parquet-glib/arrow-file-reader.h>
#include "hash_table.h"

void compute_file(const char* file,const ThreadData* data, HashTable* hash_table);

void* compute_on_thread(void* arg) {
    ThreadData* data = (ThreadData*)arg;

    HashTable* ht = create_hash_table(10);
    for(int i=0;i<data->n_files;i++) {
        compute_file(data->file_names[i], data, ht);
    }

}

void compute_file(const char* file, const ThreadData* data, HashTable* hash_table) {
    GError* error = NULL;

    GParquetArrowFileReader* reader = gparquet_arrow_file_reader_new_path(file, &error);

    
}

