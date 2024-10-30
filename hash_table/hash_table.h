//
// Created by karol on 29.10.24.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

#include "value.h"

struct hash_table {
    Value** table;
};

typedef struct hash_table HashTable;

unsigned int hash(const char* grouping_value);
Value* create_value(const char* grouping_value, Results results);
HashTable* create_table();
void insert(HashTable* hash_table, const char* grouping_value, Results results);
Results* search(HashTable* hash_table, const char* grouping_value);
void delete(HashTable* hash_table, const char* grouping_value);
void free_table(HashTable* hash_table);

#endif //HASH_TABLE_H
