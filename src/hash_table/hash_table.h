//
// Created by karol on 31.10.24.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "aggregate_function.h"
#include "query_response.pb-c.h"

typedef struct HashTableValue {
    long value;
    long count;
    AggregateFunction aggregate_function;
} HashTableValue;

typedef struct HashTableEntry {
    char* key;
    int n_values;
    HashTableValue* values;
    struct HashTableEntry* next;
} HashTableEntry;

typedef struct HashTable {
    int size;
    int entries_count;
    HashTableEntry** table;
} HashTable;

unsigned int hash(const char* string, const int table_size);
HashTable* hash_table_create(int size);
void hash_table_free(HashTable* table);
void hash_table_insert(HashTable* table, HashTableEntry* entry);
HashTableEntry* hash_table_search(HashTable* table, const char* key);
void hash_table_delete(HashTable* table, const char* key);
void hash_table_print(HashTable* ht);
void hash_table_combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2);
void hash_table_combine_table_with_response(HashTable* ht, QueryResponse* query_reponse);
void hash_table_combine_hash_tables(HashTable* destination, HashTable* source);

#endif //HASH_TABLE_H
