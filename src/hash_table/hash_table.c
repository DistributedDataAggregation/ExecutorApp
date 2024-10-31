//
// Created by karol on 31.10.24.
//

#include <stdlib.h>
#include "hash_table.h"
#include "error.h"
#include "error_utilites.h"
#include <stdio.h>
#include <string.h>

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

                  free(current->values);
                  free(current);
                  return;
            }

            prev = current;
            current = current->next;
      }
}