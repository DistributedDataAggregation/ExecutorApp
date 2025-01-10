#ifndef HASH_TABLE_INTERFACE_H
#define HASH_TABLE_INTERFACE_H
#include "hash_table_struct.h"
#include "hash_table_optimized.h"

#include "error_handling.h"
#include "query_response.pb-c.h"

typedef struct HashTableInterface
{
    unsigned int (*hash)(const char* string, int table_size);
    HashTable* (*create)(int size, int entries_limit, ErrorInfo* err);
    void (*free)(HashTable* table);
    void (*insert)(HashTable* table, HashTableEntry* entry, ErrorInfo* err);
    HashTableEntry* (*search)(const HashTable* table, const char* key);
    void (*remove)(HashTable* table, const char* key);
    void (*print)(const HashTable* ht);
    void (*combine_entries)(HashTableEntry* entry1, const HashTableEntry* entry2, ErrorInfo* err);
    HashTableValue (*update_value)(HashTableValue current_value, HashTableValue incoming_value, ErrorInfo* err);
    void (*combine_with_response)(HashTable* ht, const QueryResponse* query_response, ErrorInfo* err);
    void (*combine_hash_tables)(HashTable* destination, const HashTable* source, ErrorInfo* err);
    void (*free_entry)(HashTableEntry* value);
    QueryResponse* (*convert_to_response)(const HashTable* table, ErrorInfo* err);
} HashTableInterface;


HashTableInterface* create_default_hash_table_interface();
HashTableInterface* create_optimized_hash_table_interface();
void free_hash_table_interface(HashTableInterface* interface);

#endif //HASH_TABLE_INTERFACE_H
