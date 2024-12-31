#ifndef HASH_TABLE_ABSTRACTION_H
#define HASH_TABLE_ABSTRACTION_H

typedef struct {
    void (*insert)(void* table, const char* key, void* value);
    void* (*search)(void* table, const char* key);
    void (*remove)(void* table, const char* key);
    void (*print)(void* table);
    void (*free)(void* table);
} HashTableAbstraction;

#endif //HASH_TABLE_ABSTRACTION_H