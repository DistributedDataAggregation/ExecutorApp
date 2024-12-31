//
// Created by karol on 31.10.24.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H


#include "error_handling.h"
#include "query_response.pb-c.h"
#include "hash_table_struct.h"

unsigned int hash(const char* string, int table_size);
HashTable* hash_table_create(int size, ErrorInfo* err);
void hash_table_free(HashTable* table);
void hash_table_insert(HashTable* table, HashTableEntry* entry, ErrorInfo* err);
HashTableEntry* hash_table_search(const HashTable* table, const char* key);
void hash_table_delete(HashTable* table, const char* key);
void hash_table_print(const HashTable* ht);
void hash_table_combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2, ErrorInfo* err);
HashTableValue hash_table_update_value(HashTableValue current_value, HashTableValue incoming_value, ErrorInfo* err);
void hash_table_combine_table_with_response(HashTable* ht, const QueryResponse* query_response, ErrorInfo* err);
void hash_table_combine_hash_tables(HashTable* destination, const HashTable* source, ErrorInfo* err);

#endif //HASH_TABLE_H
