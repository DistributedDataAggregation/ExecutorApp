//
// Created by karol on 31.10.24.
//

#include <stdlib.h>
#include "hash_table.h"
#include "error.h"
#include "error_utilites.h"
#include <stdio.h>
#include <string.h>
HashTableValue update_value(HashTableValue current_value, HashTableValue incoming_value);

unsigned int hash(const char* string, const int table_size) {
      unsigned int hash_value = 0;
      while (*string) {
            hash_value = (hash_value * 31) + *string;
            string++;
      }
      return hash_value % table_size;
}

HashTable* create_hash_table(int size) {
      HashTable* hash_table = malloc(sizeof(HashTable));
      hash_table->entries_count = 0;
      if(hash_table == NULL) {
            ERR_AND_EXIT("malloc");
      }

      hash_table->size = size;
      hash_table->table = (HashTableEntry**)(malloc(sizeof(HashTableEntry*)*size));
      if(hash_table->table == NULL) {
            ERR_AND_EXIT("malloc");
      }

      for(int i = 0; i < size; i++) {
            hash_table->table[i] = NULL;
      }

      return hash_table;
}

void free_hash_table(HashTable* table) {
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

void insert(HashTable* table, HashTableEntry* entry){
      if(entry == NULL) {
            ERR_AND_EXIT("Entry is NULL");
      }

      if (table == NULL || table->table == NULL) {
            ERR_AND_EXIT("Unitialized hashtable");
      }

      unsigned int hash_value = hash(entry->key, table->size);
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

HashTableEntry* search(HashTable* table, const char* key) {
      unsigned int hash_value = hash(key, table->size);
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

void delete(HashTable* table, const char* key) {
      unsigned int hash_value = hash(key, table->size);
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

void print(HashTable* ht) {
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
                                    printf("  Value[%d]: Accumulator = %ld, Count = %ld (Average)\n", j, value->accumulator, value->count);
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

                  entry = entry->next;  // Move to the next entry in the bucket (linked list)
            }
      }
}

void combine_entries(HashTableEntry* entry1, const HashTableEntry* entry2) {
      if(entry1 == NULL || entry2 == NULL) {
            INTERNAL_ERROR("Entry1 or Entry2 is NULL");
            return;
      }

      if(entry1->n_values > entry2->n_values) {
            INTERNAL_ERROR("Entry1 and Entry2 have different number of values");
            return;
      }
      int size = entry1->n_values;

      for(int i=0; i<size; i++) {
            entry1->values[i] = update_value(entry1->values[i], entry2->values[i]);
      }
}

HashTableValue update_value(HashTableValue current_value, HashTableValue incoming_value) {
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
                  current_value.accumulator += incoming_value.accumulator;
                  current_value.count += incoming_value.count;
                  break;
            }
            case MEDIAN: {
                  // TODO: implement median calculation
                  current_value.value = -1;
                  break;
            }
            case UNKNOWN:
                  break;
      }

      return current_value;
}