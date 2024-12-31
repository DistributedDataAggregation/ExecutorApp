//
// Created by kacper on 12/31/24.
//

#ifndef HASH_TABLE_OPTIMIZED_H
#define HASH_TABLE_OPTIMIZED_H
//
// Created by karol on 31.10.24.
//

#ifndef HASH_TABLE_NEW_H
#define HASH_TABLE_NEW_H

#include <stdbool.h>

// typedef enum aggregate_function_new
// {
//     UNKNOWN_NEW = -1,
//     MIN_NEW = 0,
//     MAX_NEW = 1,
//     AVG_NEW = 2,
//     MEDIAN_NEW = 3,
// } AggregateFunctionNew;
//
// typedef struct HashTableValueNew
// {
//     long value;
//     long count;
//     AggregateFunctionNew aggregate_function;
// } HashTableValueNew;
//
// typedef struct HashTableEntryNew
// {
//     char* key;
//     int n_values;
//     HashTableValueNew* values;
//     bool is_deleted;
// } HashTableEntryNew;
//
// typedef struct HashTableNew
// {
//     int size;
//     int entries_count;
//     HashTableEntryNew** table;
// } HashTableNew;

unsigned int hash_new(const char* string, const int table_size);
HashTableNew* hash_table_create_new(int size);
void hash_table_free_new(HashTableNew* table);
void hash_table_insert_new(HashTableNew* table, HashTableEntryNew* entry);
HashTableEntryNew* hash_table_search_new(HashTableNew* table, const char* key);
void hash_table_delete_new(HashTableNew* table, const char* key);
void hash_table_print_new(HashTableNew* ht);
void hash_table_combine_entries_new(HashTableEntryNew* entry1, const HashTableEntryNew* entry2);
HashTableValueNew update_value_new(HashTableValueNew current_value, HashTableValueNew incoming_value);
void hash_table_resize_new(HashTableNew* ht);

#endif //HASH_TABLE_NEW_H


#endif //HASH_TABLE_OPTIMIZED_H
