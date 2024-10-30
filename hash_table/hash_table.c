//
// Created by karol on 29.10.24.
//

#include "hash_table.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define TABLE_SIZE 10
// TODO: change this GPT implementation into our own

// Hash function to compute the hash for a given string
unsigned int hash(const char* grouping_value) {
    unsigned int hash_value = 0;
    while (*grouping_value) {
        hash_value = (hash_value * 31) + *grouping_value;
        grouping_value++;
    }
    return hash_value % TABLE_SIZE;  // Modulus to fit within table size
}

// Create a new value node
Value* create_value(const char* grouping_value, Results results) {
    Value* new_value = (Value*)malloc(sizeof(Value));
    new_value->grouping_value = strdup(grouping_value);  // copy the string grouping_value
    new_value->results = results;  // copy the results struct
    new_value->next = NULL;
    return new_value;
}

// Create a hash table
HashTable* create_table() {
    HashTable* hash_table = (HashTable*)malloc(sizeof(HashTable));
    hash_table->table = (Value**)malloc(TABLE_SIZE * sizeof(Value*));
    for (int i = 0; i < TABLE_SIZE; i++) {
        hash_table->table[i] = NULL;  // Initialize all buckets to NULL
    }
    return hash_table;
}

// Insert a grouping_value-results pair into the hash table
void insert(HashTable* hash_table, const char* grouping_value, Results results) {
    unsigned int index = hash(grouping_value);
    Value* new_value = create_value(grouping_value, results);

    if (hash_table->table[index] == NULL) {
        // No collision, insert directly
        hash_table->table[index] = new_value;
    } else {
        // Collision resolution: Insert at the end of the linked list
        Value* current = hash_table->table[index];
        while (current->next != NULL) {
            if (strcmp(current->grouping_value, grouping_value) == 0) {
                // If the grouping_value already exists, update the results
                current->results = results;
                free(new_value);  // Free the new value since we don't need it
                return;
            }
            current = current->next;
        }

        // Check the last node's grouping_value as well
        if (strcmp(current->grouping_value, grouping_value) == 0) {
            current->results = results;  // Update existing grouping_value
            free(new_value);  // Free the new value since we don't need it
        } else {
            current->next = new_value;  // Append new value at the end of the list
        }
    }
}

// Search for a Results by grouping_value in the hash table
Results* search(HashTable* hash_table, const char* grouping_value) {
    unsigned int index = hash(grouping_value);
    Value* current = hash_table->table[index];

    while (current != NULL) {
        if (strcmp(current->grouping_value, grouping_value) == 0) {
            return &current->results;  // Return the results if the grouping_value is found
        }
        current = current->next;
    }

    return NULL;  // Return NULL if the grouping_value is not found
}

// Remove a grouping_value-results pair from the hash table
void delete(HashTable* hash_table, const char* grouping_value) {
    unsigned int index = hash(grouping_value);
    Value* current = hash_table->table[index];
    Value* prev = NULL;

    while (current != NULL) {
        if (strcmp(current->grouping_value, grouping_value) == 0) {
            // If the grouping_value is found
            if (prev == NULL) {
                // The node to be deleted is the first node in the list
                hash_table->table[index] = current->next;
            } else {
                // Bypass the node to be deleted
                prev->next = current->next;
            }
            free(current->grouping_value);  // Free the grouping_value string
            free(current);  // Free the Value node
            return;
        }
        prev = current;
        current = current->next;
    }
    printf("Grouping value not found\n");
}

// Free all memory associated with the hash table
void free_table(HashTable* hash_table) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        Value* current = hash_table->table[i];
        while (current != NULL) {
            Value* temp = current;
            current = current->next;
            free(temp->grouping_value);
            free(temp);
        }
    }
    free(hash_table->table);
    free(hash_table);
}