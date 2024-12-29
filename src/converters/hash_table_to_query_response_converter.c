//
// Created by karol on 15.11.24.
//

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash_table_to_query_response_converter.h"

PartialResult* convert_value(HashTableValue value, ErrorInfo* err);
Value* convert_entry(const HashTableEntry* entry, ErrorInfo* err);
void free_values(Value** values, int count);

// TODO check for memory leaks

QueryResponse* convert_hash_table_to_query_response(const HashTable *table, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return NULL;
    }

    QueryResponse* query_response = malloc(sizeof(QueryResponse));
    if (query_response == NULL) {
        LOG_ERR("Failed to allocate for query response while converting");
        SET_ERR(err, errno, "Failed to allocate for query response while converting", strerror(errno));
        return NULL;
    }
    query_response__init(query_response);

    const int values_counter = table->entries_count;
    int index_of_non_empty = 0;
    HashTableEntry* entry = table->table[index_of_non_empty];

    while(index_of_non_empty + 1 <= table->size && entry == NULL) {
        index_of_non_empty++;
        entry = table->table[index_of_non_empty];
    }

    if(index_of_non_empty == table->size) {
        LOG_INTERNAL_ERR("No entries to convert to query response\n");
        return query_response;
    }

    query_response->n_values = values_counter;
    query_response->values = malloc(sizeof(Value*)*query_response->n_values);
    if(query_response->values == NULL) {
        LOG_ERR("Failed to allocate for query response values while converting");
        SET_ERR(err, errno, "Failed to allocate for query response values while converting", strerror(errno));
        query_response->n_values = 0;
        free(query_response);
        return NULL;
    }

    int converted_values = 0;
    for(int i = index_of_non_empty; i < table->size; i++) {
        HashTableEntry* current = table->table[i];
        while(current != NULL) {
            if(converted_values < query_response->n_values) {
                query_response->values[converted_values] = convert_entry(current, err);
                if (err->error_code != NO_ERROR) {
                    LOG_INTERNAL_ERR("Failed to convert query response values\n");
                    SET_ERR(err, INTERNAL_ERROR, "Failed to convert query response values", "");
                    query_response->n_values = 0;
                    free_values(query_response->values, converted_values);
                    free(query_response);
                    return NULL;
                }
                converted_values++;
                current = current->next;
            }
            else {
                LOG_INTERNAL_ERR("Too many values to convert");
                break;
            }
        }
    }

    if(converted_values != values_counter) {
        LOG_INTERNAL_ERR("Wrong number of values converted to query response\n");
        // TODO send failure or converted ones (now)?
    }

    return query_response;
}

Value* convert_entry(const HashTableEntry* entry, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return NULL;
    }

    Value* value = malloc(sizeof(Value));
    if (value == NULL) {
        LOG_ERR("Failed to allocate for query response value\n");
        SET_ERR(err, errno, "Failed to allocate for query response value", strerror(errno));
        return NULL;
    }
    value__init(value);

    value->n_results = entry->n_values;
    value->grouping_value = strdup(entry->key);
    value->results = malloc(sizeof(PartialResult*) * value->n_results);
    if (value->results == NULL) {
        LOG_ERR("Failed to allocate for query response value results\n");
        SET_ERR(err, errno, "Failed to allocate for query response value results", strerror(errno));
        free(value);
        return NULL;
    }

    for(int i=0; i<entry->n_values; i++) {
        value->results[i] = convert_value(entry->values[i], err);
        if (err->error_code != NO_ERROR) {
            for (int j = 0; j < i; j++) {
                free(value->results[j]);
            }
            free(value->results);
            return NULL;
        }
    }

    return value;
}

PartialResult* convert_value(const HashTableValue value, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return NULL;
    }

    PartialResult* result = malloc(sizeof(PartialResult));
    if (result == NULL) {
        LOG_ERR("Failed to allocate for query response value partial result\n");
        SET_ERR(err, errno, "Failed to allocate for query response value partial resul", strerror(errno));
        return NULL;
    }

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

void free_values(Value** values, const int count) {
    for (int i = 0; i < count; i++) {
        for (int j = 0; j < values[i]->n_results; j++) {
            free(values[i]->results[j]);
        }
        free(values[i]->results);
    }
    free(values);
}