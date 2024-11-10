//
// Created by karol on 31.10.24.
//

#ifndef HASH_TABLE_H
#define HASH_TABLE_H

typedef enum ResultType {
    UNKNOWN_RESULT,
    SINGLE_RESULT,
    COUNTED_RESULT,
}ResultType;

typedef struct HashTableValue {
    union {
        long value;
        struct {
            long accumulator;
            long count;
        };
    };
    ResultType result_type;
} HashTableValue;

typedef struct HashTableEntry {
    char* key;
    int n_values;
    HashTableValue* values;
    struct HashTableEntry* next;
} HashTableEntry;

typedef struct HashTable {
    int size;
    HashTableEntry** table;
} HashTable;

unsigned int hash(const char* string, const int table_size);
HashTable* create_hash_table(int size);
void free_hash_table(HashTable* table);
void insert(HashTable* table, HashTableEntry* entry);
HashTableEntry* search(HashTable* table, const char* key);
void delete(HashTable* table, const char* key);
void print(HashTable* ht);

#endif //HASH_TABLE_H
