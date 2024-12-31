#ifndef HASH_TABLE_STRUCT_H
#define HASH_TABLE_STRUCT_H

#include "aggregate_function.h"
#include <stdbool.h>

typedef struct HashTableValue
{
    long value;
    long count;
    AggregateFunction aggregate_function;
} HashTableValue;

typedef struct HashTableEntry
{
    char* key;
    int n_values;
    HashTableValue* values;
    struct HashTableEntry* next;
    bool is_deleted;
} HashTableEntry;

typedef struct HashTable
{
    int size;
    int entries_count;
    HashTableEntry** table;
} HashTable;


#endif //HASH_TABLE_STRUCT_H
