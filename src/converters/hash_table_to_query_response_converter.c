//
// Created by karol on 15.11.24.
//

#include "hash_table_to_query_response_converter.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error_utilites.h"

Result* convert_value(HashTableValue value);
Value* convert_entry(HashTableEntry* entry);

QueryResponse* convert_table(HashTable *table) {
    QueryResponse* query_response = malloc(sizeof(QueryResponse));
    query_response__init(query_response);

    int values_counter = 0;
    for(int i=0;i<table->size;i++) {
        HashTableEntry* current = table->table[i];
        while(current != NULL) {
            values_counter++;
            current = current->next;
        }
    }

    query_response->n_values = values_counter;
    query_response->values = (malloc(sizeof(Value*)*query_response->n_values));
    if(query_response->values == NULL) {
        REPORT_ERR("malloc");
        query_response->n_values = 0;
        return query_response;
    }

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

    //query_response.n_operations = entry->n_values;
    query_response->n_operations = 0;
    query_response->operations = NULL;

    int converted_values = 0;
    for(int i = index_of_non_empty; i < table->size; i++) {
        HashTableEntry* current = table->table[i];
        while(current != NULL) {
            query_response->values[converted_values] = convert_entry(current);

            converted_values++;
            current = current->next;
        }
    }
    if(converted_values != values_counter) {
        fprintf(stderr, "Wrong number of values converted to query response\n");
    }

    return query_response;
}

Value* convert_entry(HashTableEntry* entry) {
    Value* value = malloc(sizeof(Value));
    value__init(value);

    value->n_results = entry->n_values;
    value->grouping_value = strdup(entry->key);

    value->results = malloc(sizeof(Result*) * value->n_results);
    for(int i=0; i<entry->n_values; i++) {
        value->results[i] = convert_value(entry->values[i]);
    }

    return value;
}

Result* convert_value(HashTableValue value) {
    Result* result = malloc(sizeof(Result));
    result__init(result);

    switch (value.aggregate_function) {
        case MIN:
        case MAX: {
            result->result_types_case = RESULT__RESULT_TYPES_SINGLE_RESULT;
            result->singleresult = value.value;
        }
        case AVG: {
            result ->result_types_case = RESULT__RESULT_TYPES_COUNTED_RESULT;
            result->countedresult = malloc(sizeof(CountedResult));
            result->countedresult->count = value.count;
            result->countedresult->value = value.accumulator;
            break;
        }
        default:
            result->result_types_case = RESULT__RESULT_TYPES__NOT_SET;
            return NULL;
    }

    return result;
}