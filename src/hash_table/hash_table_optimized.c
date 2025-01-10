//
// Created by kapiszon on 32.12.24.
//

#include "hash_table_optimized.h"
#include <errno.h>
#include <hash_table.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "farmhash-c.h"
#include "error.h"

void hash_table_resize_new(HashTable* ht);

unsigned int hash_farm(const char* string, int table_size)
{
    size_t length = strlen(string);
    uint64_t h64 = farmhash64(string, length);
    return h64 & (table_size - 1);
}

HashTable* hash_table_optimized_create(int size, int entries_limit, ErrorInfo* err)
{
    HashTable* hash_table = malloc(sizeof(HashTable));
    if (hash_table == NULL)
    {
        return NULL;
    }

    hash_table->entries_count = 0;
    hash_table->size = size;
    hash_table->entries_limit = entries_limit;
    hash_table->table = (HashTableEntry**)(malloc(sizeof(HashTableEntry*) * size));

    if (hash_table->table == NULL)
    {
        return NULL;
    }

    for (int i = 0; i < size; i++)
    {
        hash_table->table[i] = NULL;
    }

    return hash_table;
}

void hash_table_optimized_free(HashTable* table)
{
    if (table == NULL)
    {
        return;
    }

    for (int i = 0; i < table->size; i++)
    {
        HashTableEntry* entry = table->table[i];
        if (table->table[i] != NULL && !table->table[i]->is_deleted)
        {
            free(entry->key);
            free(entry->values);
            free(entry);
        }
        else if (table->table[i] != NULL && table->table[i]->is_deleted)
        {
            free(entry);
        }
    }

    free(table->table);
    free(table);
}

void hash_table_optimized_insert(HashTable* table, HashTableEntry* entry, ErrorInfo* err)
{
    // if (err == NULL)
    // {
    //       LOG_INTERNAL_ERR("Passed error info was NULL");
    //       return;
    // }

    if (entry == NULL)
    {
        LOG_INTERNAL_ERR("Failed to insert to a hash table: Entry was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Entry was NULL");
        return;
    }
    if (entry->key == NULL)
    {
        LOG_INTERNAL_ERR("Failed to insert to a hash table: Entry key was NULL");
        SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Entry key was NULL");
        return;
    }

    if (table == NULL || table->table == NULL)
    {
        LOG_INTERNAL_ERR("Failed to insert to a hash table: Uninitialized hash table");
        SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Uninitialized hash table");
        return;
    }
    if (table->entries_count >= table->entries_limit)
    {
        LOG_ERR("Failed to insert to a hash table: Hash table is full");
        SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Hash table is full");
        return;
    }


    if (table->entries_count * 2 > (table->size))
    {
        hash_table_resize_new(table);
    }

    unsigned int hash_value = hash_farm(entry->key, table->size);

    while (table->table[hash_value] != NULL && !table->table[hash_value]->is_deleted)
    {
        hash_value = (hash_value + 1) & (table->size - 1);
    }

    table->table[hash_value] = entry;
    table->table[hash_value]->is_deleted = false;
    table->entries_count++;
}


void hash_table_resize_new(HashTable* ht)
{
    int new_size = ht->size * 2;
    HashTableEntry** new_table = calloc(new_size, sizeof(HashTableEntry*));
    if (new_table == NULL)
    {
        return;
    }

    HashTableEntry** old_table = ht->table;
    int old_size = ht->size;
    ht->table = new_table;
    ht->size = new_size;
    ht->entries_count = 0;

    for (int i = 0; i < old_size; i++)
    {
        if (old_table[i] != NULL)
        {
            hash_table_optimized_insert(ht, old_table[i], NULL);
        }
    }

    free(old_table);
}

HashTableEntry* hash_table_optimized_search(const HashTable* table, const char* key)
{
    unsigned int hash_value = hash_farm(key, table->size);
    if (table->table[hash_value] == NULL)
    {
        return NULL;
    }

    unsigned int i = 0;
    HashTableEntry* entry = table->table[(hash_value + i) & (table->size - 1)];
    while (entry != NULL)
    {
        if (strcmp(entry->key, key) == 0)
        {
            return entry;
        }

        i++;
        entry = table->table[(hash_value + i) & (table->size - 1)];
    }

    return NULL;
}

void hash_table_optimized_delete(HashTable* table, const char* key)
{
    if (table == NULL || table->table == NULL || key == NULL)
    {
        return;
    }

    unsigned int hash_value = hash_farm(key, table->size);

    while (table->table[hash_value] != NULL)
    {
        HashTableEntry* current = table->table[hash_value];

        if (!current->is_deleted && strcmp(current->key, key) == 0)
        {
            free(current->key);
            free(current->values);

            current->is_deleted = true;
            table->entries_count--;
            return;
        }

        hash_value = (hash_value + 1) & (table->size - 1);
    }
}

void hash_table_optimized_print(const HashTable* ht)
{
    if (ht == NULL || ht->table == NULL)
    {
        printf("HashTable is empty.\n");
        return;
    }

    for (int i = 0; i < ht->size; ++i)
    {
        HashTableEntry* entry = ht->table[i];

        if (entry == NULL)
        {
            continue;
        }
        printf("Key: %s\n", entry->key);
        printf("Values:\n");

        for (int j = 0; j < entry->n_values; ++j)
        {
            HashTableValue* value = &entry->values[j];

            switch (value->aggregate_function)
            {
            case MIN:
                {
                    printf("  Value[%d]: %ld (Min)\n", j, value->value);
                    break;
                }
            case MAX:
                {
                    printf("  Value[%d]: %ld (Max)\n", j, value->value);
                    break;
                }
            case AVG:
                {
                    printf("  Value[%d]: Accumulator = %ld, Count = %ld (Average)\n", j, value->value,
                           value->count);
                    break;
                }
            case MEDIAN:
                {
                    printf("  Value[%d]: %ld (Max)\n", j, value->value);
                    break;
                }
            case SUM:
                {
                    printf("  Value[%d]: %ld (Sum)\n", j, value->value);
                    break;
                }
            case COUNT:
                {
                    printf("  Value[%d]: %ld (Count)\n", j, value->value);
                    break;
                }
            case UNKNOWN:
                {
                    printf("  Value[%d]: (Unknown)\n", j);
                    break;
                }
            }
        }
        printf("\n");
    }
}


void hash_table_combine_table_with_response_optimized(HashTable* ht, const QueryResponse* query_response,
                                                      ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }

    for (int i = 0; i < query_response->n_values; i++)
    {
        const Value* current = query_response->values[i];
        HashTableValue* values = malloc(sizeof(HashTableValue) * current->n_results);
        if (values == NULL)
        {
            LOG_ERR("Failed to allocate memory for hash table values");
            SET_ERR(err, errno, "Failed to allocate memory for hash table values", strerror(errno));
            return;
        }

        for (int j = 0; j < current->n_results; j++)
        {
            values[j] = map_partial_result_to_table_value(current->results[j], err);
        }

        HashTableEntry* entry = malloc(sizeof(HashTableEntry));
        if (entry == NULL)
        {
            LOG_ERR("Failed to allocate memory for hash table entry");
            SET_ERR(err, errno, "Failed to allocate memory for hash table entry", strerror(errno));
            free(values);
            return;
        }

        entry->key = strdup(current->grouping_value);
        entry->n_values = (int)current->n_results;
        entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
        if (entry->values == NULL)
        {
            LOG_ERR("Failed to allocate memory for new hash table entry values");
            SET_ERR(err, errno, "Failed to allocate memory for new hash table entry values",
                    strerror(errno));
            free(entry->key);
            free(entry);
            return;
        }
        memcpy(entry->values, values, sizeof(HashTableValue) * entry->n_values);
        entry->is_deleted = entry->is_deleted;

        HashTableEntry* found = hash_table_optimized_search(ht, entry->key);
        if (found == NULL)
        {
            hash_table_optimized_insert(ht, entry, err);
            if (err->error_code != NO_ERROR)
            {
                free(values);
                free(entry);
                return;
            }
        }
        else
        {
            hash_table_combine_entries(found, entry, err);
            if (err->error_code != NO_ERROR)
            {
                free(values);
                free(entry);
                return;
            }
            hash_table_free_entry(entry);
        }
    }
}

void hash_table_optimized_combine_hash_tables(HashTable* destination, const HashTable* source, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }
    destination->entries_limit += source->entries_limit;

    for (int i = 0; i < source->size; i++)
    {
        HashTableEntry* entry = source->table[i];

        if (entry == NULL)
        {
            continue;
        }

        HashTableEntry* found = hash_table_optimized_search(destination, entry->key);

        if (found == NULL)
        {
            HashTableEntry* new_entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
            if (new_entry == NULL)
            {
                LOG_ERR("Failed to allocate memory for new hash table entry");
                SET_ERR(err, errno, "Failed to allocate memory for new hash table entry", strerror(errno));
                return;
            }
            new_entry->key = strdup(entry->key);
            new_entry->n_values = entry->n_values;
            new_entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
            if (new_entry->values == NULL)
            {
                LOG_ERR("Failed to allocate memory for new hash table entry values");
                SET_ERR(err, errno, "Failed to allocate memory for new hash table entry values",
                        strerror(errno));
                free(new_entry->key);
                free(new_entry);
                return;
            }
            memcpy(new_entry->values, entry->values, sizeof(HashTableValue) * entry->n_values);
            new_entry->is_deleted = entry->is_deleted;

            hash_table_optimized_insert(destination, new_entry, err);
            if (err->error_code != NO_ERROR)
            {
                free(new_entry->key);
                free(new_entry->values);
                free(new_entry);
                return;
            }
        }
        else
        {
            hash_table_combine_entries(found, entry, err);
            if (err->error_code != NO_ERROR)
            {
                return;
            }
        }
    }
}
