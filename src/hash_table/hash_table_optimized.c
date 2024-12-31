//
// Created by kacper on 12/31/24.
//

#include "hash_table_optimized.h"

//
// Created by kapiszon on 32.12.24.
//
#include <errno.h>
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

HashTable* hash_table_optimized_create(int size)
{
      HashTable* hash_table = malloc(sizeof(HashTable));
      if (hash_table == NULL)
      {
            return NULL;
      }

      hash_table->entries_count = 0;
      hash_table->size = size;
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
      if (entry == NULL)
      {
            return;
      }

      if (table == NULL || table->table == NULL)
      {
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
            if (ht->table[i] != NULL)
            {
                  //  hash_table_optimized_insert(ht, old_table[i],);
            }
      }

      free(old_table);
}

HashTableEntry* hash_table_optimized_search(HashTable* table, const char* key)
{
      unsigned int hash_value = hash_farm(key, table->size);
      if (table->table[hash_value] == NULL)
      {
            return NULL;
      }

      unsigned int i = 1;
      HashTableEntry* entry = table->table[(hash_value + i) & (table->size - 1)];
      while (entry != NULL)
      {
            if (strcmp(entry->key, key) == 0)
            {
                  return entry;
            }

            i++;
            // i = i << 1
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

void hash_table_optimized_print(HashTable* ht)
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


void hash_table_optimized_combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2, ErrorInfo* err)
{
      if (err == NULL)
      {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      if (entry1 == NULL || entry2 == NULL)
      {
            LOG_INTERNAL_ERR("Failed to combine hash table entries: At least one of combined entries was NULL");
            SET_ERR(err, INTERNAL_ERROR, "Failed to combine hash table entries",
                    "At least one of combined entries was NULL");
            return;
      }

      if (entry1->n_values > entry2->n_values)
      {
            LOG_INTERNAL_ERR("Failed to combine hash table entries: Entries have different number of values");
            SET_ERR(err, INTERNAL_ERROR, "Failed to combine hash table entries",
                    "Entries have different number of values");
            return;
      }
      const int size = entry1->n_values;

      for (int i = 0; i < size; i++)
      {
            entry1->values[i] = hash_table_optimized_update_value(entry1->values[i], entry2->values[i], err);
            if (err->error_code != NO_ERROR)
            {
                  return;
            }
      }
}

HashTableValue hash_table_optimized_update_value(HashTableValue current_value, HashTableValue incoming_value,
                                                 ErrorInfo* err)
{
      switch (current_value.aggregate_function)
      {
      case MIN:
            {
                  current_value.value = incoming_value.value < current_value.value
                                              ? incoming_value.value
                                              : current_value.value;
                  break;
            }
      case MAX:
            {
                  current_value.value = incoming_value.value > current_value.value
                                              ? incoming_value.value
                                              : current_value.value;
                  break;
            }
      case AVG:
            {
                  current_value.value += incoming_value.value;
                  current_value.count += incoming_value.count;
                  break;
            }
      case MEDIAN:
            {
                  // TODO: implement median calculation
                  current_value.value = -1;
                  break;
            }
      case UNKNOWN:
            break;
      }

      return current_value;
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
                  values[j].value = current->results[j]->value;
                  values[j].count = current->results[j]->count;
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
            entry->values = values;

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
                  hash_table_optimized_combine_entries(found, entry, err);
                  if (err->error_code != NO_ERROR)
                  {
                        free(values);
                        free(entry);
                        return;
                  }
            }

            free(values);
            free(entry);
      }
}

void hash_table_optimized_combine_hash_tables(HashTable* destination, const HashTable* source, ErrorInfo* err)
{
      if (err == NULL)
      {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      for (int i = 0; i < source->size; i++)
      {
            HashTableEntry* entry = source->table[i];
            while (entry != NULL)
            {
                  HashTableEntry* next = entry->next;

                  HashTableEntry* found = hash_table_optimized_search(destination, entry->key);
                  if (found == NULL)
                  {
                        entry->next = NULL;
                        hash_table_optimized_insert(destination, next, err);
                        if (err->error_code != NO_ERROR)
                        {
                              return;
                        }
                  }
                  else
                  {
                        hash_table_optimized_combine_entries(found, entry, err);
                        if (err->error_code != NO_ERROR)
                        {
                              return;
                        }
                  }

                  entry = next;
            }
      }
}
