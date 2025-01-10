#ifndef HASH_TABLE_STRUCT_H
#define HASH_TABLE_STRUCT_H

#include "aggregate_function.h"
#include <stdbool.h>

typedef enum HashTableValueType
{
    HASH_TABLE_UNSUPPORTED,
    HASH_TABLE_INT,
    HASH_TABLE_FLOAT,
    HASH_TABLE_DOUBLE,
} HashTableValueType;

typedef struct HashTableValue
{
    long count;
    int is_null;
    HashTableValueType type;

    union
    {
        long value;
        float float_value;
        double double_value;
    };

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
    int max_size;
} HashTable;

#endif //HASH_TABLE_STRUCT_H
