//
// Created by kacper on 12/31/24.
//

#ifndef HASH_TABLE_OPTIMIZED_H
#define HASH_TABLE_OPTIMIZED_H


#include <hash_table_struct.h>
#include <query_response.pb-c.h>
#include "error_handling.h"

unsigned int hash_farm(const char* string, const int table_size);

HashTable* hash_table_optimized_create(int size, ErrorInfo* err);

void hash_table_optimized_free(HashTable* table);

void hash_table_optimized_insert(HashTable* table, HashTableEntry* entry, ErrorInfo* err);

HashTableEntry* hash_table_optimized_search(const HashTable* table, const char* key);

void hash_table_optimized_delete(HashTable* table, const char* key);

void hash_table_optimized_print(const HashTable* ht);

void hash_table_optimized_combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2, ErrorInfo* err);

HashTableValue hash_table_optimized_update_value(HashTableValue current_value, HashTableValue incoming_value,
                                                 ErrorInfo* err);

void hash_table_combine_table_with_response_optimized(HashTable* ht, const QueryResponse* query_response,
                                                      ErrorInfo* err);

void hash_table_optimized_combine_hash_tables(HashTable* destination, const HashTable* source, ErrorInfo* err);


#endif //HASH_TABLE_OPTIMIZED_H
