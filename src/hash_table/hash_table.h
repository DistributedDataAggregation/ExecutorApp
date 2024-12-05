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
HashTable* create_hash_table(int size);
void free_hash_table(HashTable* table);
void insert(HashTable* table, HashTableEntry* entry);
HashTableEntry* search(HashTable* table, const char* key);
void delete(HashTable* table, const char* key);
void print(HashTable* ht);
void combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2);
void combine_table_with_response(HashTable* ht, QueryResponse* query_reponse);

#endif //HASH_TABLE_H
