//
// Created by karol on 31.10.24.
//

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash_table.h"

unsigned int hash(const char* string, const int table_size) {
      unsigned int hash_value = 0;
      while (*string) {
            hash_value = (hash_value * 31) + *string;
            string++;
      }
      return hash_value % table_size;
}

void hash_table_free_entry(HashTableEntry* value) {
      free(value->key);
      free(value->values);
      free(value);
      value = NULL;
}

HashTable* hash_table_create(const int size, ErrorInfo* err) {

      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return NULL;
      }

      HashTable* hash_table = malloc(sizeof(HashTable));
      if(hash_table == NULL) {
            LOG_ERR("Failed to allocate memory for a hash table");
            SET_ERR(err, errno, "Failed to allocate memory for a hash table", strerror(errno));
            return NULL;
      }

      hash_table->entries_count = 0;
      hash_table->size = size;
      hash_table->table = (HashTableEntry**)(malloc(sizeof(HashTableEntry*)*size));
      if(hash_table->table == NULL) {
            LOG_ERR("Failed to allocate memory for a hash table table");
            SET_ERR(err, errno, "Failed to allocate memory for a hash table table", strerror(errno));
            free(hash_table);
            return NULL;
      }

      for(int i = 0; i < size; i++) {
            hash_table->table[i] = NULL;
      }

      return hash_table;
}

void hash_table_free(HashTable* table) {

      if (table == NULL) {
            return;
      }

      for(int i = 0; i < table->size; i++) {
            if(table->table[i] != NULL) {
                  HashTableEntry* entry = table->table[i];
                  while(entry != NULL) {
                        HashTableEntry* next = entry->next;
                        free(entry->key);
                        free(entry->values);
                        free(entry);
                        entry = next;
                  }
            }
      }

      free(table->table);
      free(table);
}

void hash_table_insert(HashTable* table, HashTableEntry* entry, ErrorInfo* err){

      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      if(entry == NULL) {
            LOG_INTERNAL_ERR("Failed to insert to a hash table: Entry was NULL");
            SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Entry was NULL");
            return;
      }

      if (table == NULL || table->table == NULL) {
            LOG_INTERNAL_ERR("Failed to insert to a hash table: Uninitialized hash table");
            SET_ERR(err, INTERNAL_ERROR, "Failed to insert to a hash table", "Uninitialized hash table");
            return;
      }

      const unsigned int hash_value = hash(entry->key, table->size);
      if(table->table[hash_value] == NULL) {
            table->table[hash_value] = entry;
            table->table[hash_value]->next = NULL;
      } else {
            HashTableEntry* current = table->table[hash_value];

            while(current->next != NULL) {
                  current = current->next;
            }

            current->next = entry;
      }
      table->entries_count++;
}

HashTableEntry* hash_table_search(const HashTable* table, const char* key) {
      const unsigned int hash_value = hash(key, table->size);
      if(table->table[hash_value] == NULL) {
            return NULL;
      }

      HashTableEntry* entry = table->table[hash_value];
      while(entry != NULL) {
            if(strcmp(entry->key, key) == 0) {
                  return entry;
            }
            entry = entry->next;
      }

      return NULL;
}

void hash_table_delete(HashTable* table, const char* key) {
      const unsigned int hash_value = hash(key, table->size);
      if(table->table[hash_value] == NULL) {
            return;
      }

      HashTableEntry* current = table->table[hash_value];
      HashTableEntry* prev = NULL;
      while(current != NULL) {
            if(strcmp(current->key, key) == 0) {
                  if(prev == NULL) {
                        table->table[hash_value] = current->next;
                  }
                  else {
                        prev->next = current->next;
                  }

                  free(current->key);
                  current->key = NULL;
                  free(current->values);
                  current->values = NULL;
                  free(current);
                  current = NULL;
                  table->entries_count--;

                  return;
            }

            prev = current;
            current = current->next;
      }
}

void hash_table_print(const HashTable* ht) {
      if (ht == NULL || ht->table == NULL) {
            printf("HashTable is empty.\n");
            return;
      }

      for (int i = 0; i < ht->size; ++i) {
            HashTableEntry* entry = ht->table[i];
            while (entry != NULL) {
                  printf("Key: %s\n", entry->key);
                  printf("Values:\n");

                  for (int j = 0; j < entry->n_values; ++j) {
                        HashTableValue* value = &entry->values[j];

                        switch(value->aggregate_function) {
                              case MIN: {
                                    printf("  Value[%d]: %ld (Min)\n", j, value->value);
                                    break;
                              }
                              case MAX: {
                                    printf("  Value[%d]: %ld (Max)\n", j, value->value);
                                    break;
                              }
                              case AVG:{
                                    printf("  Value[%d]: Accumulator = %ld, Count = %ld (Average)\n", j, value->value, value->count);
                                    break;
                              }
                              case MEDIAN:{
                                    printf("  Value[%d]: %ld (Max)\n", j, value->value);
                                    break;
                              }
                              case UNKNOWN: {
                                    printf("  Value[%d]: (Unknown)\n", j);
                                    break;
                              }
                        }
                  }
                  printf("\n");

                  entry = entry->next;
            }
      }
}

void hash_table_combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2, ErrorInfo* err) {

      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      if(entry1 == NULL || entry2 == NULL) {
            LOG_INTERNAL_ERR("Failed to combine hash table entries: At least one of combined entries was NULL");
            SET_ERR(err, INTERNAL_ERROR, "Failed to combine hash table entries",
                  "At least one of combined entries was NULL");
            return;
      }

      if(entry1->n_values != entry2->n_values) {
            LOG_INTERNAL_ERR("Failed to combine hash table entries: Entries have different number of values");
            SET_ERR(err, INTERNAL_ERROR, "Failed to combine hash table entries",
                  "Entries have different number of values");
            return;
      }
      const int size = entry1->n_values;

      for(int i=0; i<size; i++) {
            entry1->values[i] = hash_table_update_value(entry1->values[i], entry2->values[i], err);
            if (err->error_code != NO_ERROR) {
                  return;
            }
      }
}

HashTableValue hash_table_update_value(HashTableValue current_value, const HashTableValue incoming_value, ErrorInfo* err) {

      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return current_value;
      }

      if (current_value.is_null) {
            return incoming_value;
      } else if (incoming_value.is_null) {
            return current_value;
      }

      switch (current_value.aggregate_function) {
            case MIN: {
                  current_value.value = incoming_value.value < current_value.value ? incoming_value.value : current_value.value;
                  break;
            }
            case MAX: {
                  current_value.value = incoming_value.value > current_value.value ? incoming_value.value : current_value.value;
                  break;
            }
            case AVG: {
                  current_value.value += incoming_value.value;
                  current_value.count += incoming_value.count;
                  break;
            }
            case MEDIAN: {
                  // TODO: implement median calculation
                  current_value.value = -1;
                  break;
            }
            case UNKNOWN:
                  LOG_INTERNAL_ERR("Unsupported aggregate function");
                  SET_ERR(err, INTERNAL_ERROR, "Unsupported aggregate function", "");
                  break;
      }

      return current_value;
}

void hash_table_combine_table_with_response(HashTable* ht, const QueryResponse* query_response, ErrorInfo* err)
{
      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      for(int i=0; i<query_response->n_values; i++) {
            const Value* current = query_response->values[i];
            HashTableValue* values = malloc(sizeof(HashTableValue)*current->n_results);
            if (values == NULL) {
                  LOG_ERR("Failed to allocate memory for hash table values");
                  SET_ERR(err, errno, "Failed to allocate memory for hash table values", strerror(errno));
                  return;
            }
            for(int j=0; j<current->n_results; j++) {
                  values[j].value = current->results[j]->value;
                  values[j].count = current->results[j]->count;
                  values[j].is_null = current->results[j]->is_null;
            }

            HashTableEntry* entry = malloc(sizeof(HashTableEntry));
            if (entry == NULL) {
                  LOG_ERR("Failed to allocate memory for hash table entry");
                  SET_ERR(err, errno, "Failed to allocate memory for hash table entry", strerror(errno));
                  free(values);
                  return;
            }

            entry->key = strdup(current->grouping_value);
            entry->n_values = (int)current->n_results;
            entry->values = values;

            HashTableEntry* found = hash_table_search(ht, entry->key);
            if(found == NULL) {
                  hash_table_insert(ht, entry, err);
                  if (err->error_code != NO_ERROR) {
                        free(values);
                        free(entry);
                        return;
                  }
            } else {
                  hash_table_combine_entries(found, entry, err);
                  if (err->error_code != NO_ERROR) {
                        free(values);
                        free(entry);
                        return;
                  }
            }

            free(values);
            free(entry);
      }
}

void hash_table_combine_hash_tables(HashTable* destination, HashTable* source, ErrorInfo* err) {

      if (err == NULL) {
            LOG_INTERNAL_ERR("Passed error info was NULL");
            return;
      }

      HashTableEntry* to_be_freed = NULL;

      for(int i=0;i<source->size;i++) {
            HashTableEntry* entry = source->table[i];
            while(entry != NULL) {
                  HashTableEntry* next = entry->next;

                  HashTableEntry* found = hash_table_search(destination, entry->key);
                  if(found == NULL) {
                        entry->next = NULL;
                        hash_table_insert(destination, entry, err);
                        if (err->error_code != NO_ERROR) {
                              return;
                        }
                        to_be_freed = NULL;
                  } else {
                        hash_table_combine_entries(found, entry, err);
                        to_be_freed = entry;
                        if (err->error_code != NO_ERROR) {
                              return;
                        }
                  }
                  entry = next;

                  if (to_be_freed != NULL) {
                        hash_table_free_entry(to_be_freed);
                  }
            }
      }
}