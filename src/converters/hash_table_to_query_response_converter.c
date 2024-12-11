//
// Created by karol on 15.11.24.
//

#include "hash_table_to_query_response_converter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error_utilites.h"

PartialResult* convert_value(HashTableValue value);
Value* convert_entry(const HashTableEntry* entry);

QueryResponse* convert_hash_table_to_query_response(HashTable *table) {
    // query response init
    QueryResponse* query_response = malloc(sizeof(QueryResponse));
    query_response__init(query_response);

    // check if table is empty
    int values_counter = table->entries_count;
    int index_of_non_empty = 0;
    HashTableEntry* entry = table->table[index_of_non_empty];

    while(index_of_non_empty + 1 <= table->size && entry == NULL ) {
        index_of_non_empty++;
        entry = table->table[index_of_non_empty];
    }

    if(index_of_non_empty == table->size) {
        fprintf(stderr, "No entries to convert to query response\n");
        return query_response;
    }

    query_response->n_values = values_counter;
    query_response->values = malloc(sizeof(Value*)*query_response->n_values);
    if(query_response->values == NULL) {
        REPORT_ERR("malloc");
        query_response->n_values = 0;
        return query_response;
    }

    int converted_values = 0;
    for(int i = index_of_non_empty; i < table->size; i++) {
        HashTableEntry* current = table->table[i];
        while(current != NULL) {
            if(converted_values < query_response->n_values) {
                query_response->values[converted_values] = convert_entry(current);
                converted_values++;
                current = current->next;
            }
            else {
                INTERNAL_ERROR("convert_entry, too many values to convert");
                break;
            }
        }
    }

    if(converted_values != values_counter) {
        fprintf(stderr, "Wrong number of values converted to query response\n");
    }

    return query_response;
}

Value* convert_entry(const HashTableEntry* entry) {
    Value* value = malloc(sizeof(Value));
    value__init(value);

    value->n_results = entry->n_values;
    value->grouping_value = strdup(entry->key);
    value->results = malloc(sizeof(PartialResult*) * value->n_results);
    for(int i=0; i<entry->n_values; i++) {
        value->results[i] = convert_value(entry->values[i]);
    }

    return value;
}

PartialResult* convert_value(HashTableValue value) {
    PartialResult* result = malloc(sizeof(PartialResult));
    partial_result__init(result);

    switch (value.aggregate_function) {
        case MIN:
        case MAX: {
            result->value = value.value;
            break;
        }
        case AVG: {
            result->count = value.count;
            result->value = value.value;
            break;
        }
        default:
            result->value = 0;
            break;
    }

    return result;
}