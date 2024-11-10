//
// Created by karol on 31.10.24.
//
#include "test.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash_table.h"

void test_create_hash_table();
void test_add_to_hash_table();

void run_all_tests() {
    printf("Running tests...\n");
    test_create_hash_table();
    test_add_to_hash_table();
    printf("Finished tests\n");
}

void test_create_hash_table() {
    HashTable* ht = create_hash_table(10);
    assert(ht);
    assert(ht->size == 10);
}

void test_add_to_hash_table() {
    HashTable* ht = create_hash_table(10);

    const char* key = "war1|war2|war3|war4|war5";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue)*entry->n_values);
    for (int i = 0; i < entry->n_values; i++) {
        entry->values[i].aggregate_function = MIN;
        entry->values[i].value = 100;
    }

    insert(ht, entry);
    HashTableEntry* returned = search(ht,key);

    assert(returned != NULL);
    assert(returned->n_values == entry->n_values);
    assert(strcmp(returned->key, key) == 0);
    for (int i = 0; i < entry->n_values; i++) {
        assert(returned->values[i].aggregate_function == entry->values[i].aggregate_function);
        assert(returned->values[i].value == entry->values[i].value);
    }
    returned->values[0].value += 100;

    HashTableEntry* returned2 = search(ht, key);
    assert(returned2 != NULL);
    assert(returned2->n_values == entry->n_values);
    assert(returned2->values[0].value == 200);

    delete(ht, key);
    HashTableEntry* returned3 = search(ht, key);
    assert(returned3 == NULL);
}