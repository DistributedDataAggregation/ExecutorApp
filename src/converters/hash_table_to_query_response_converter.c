//
// Created by karol on 15.11.24.
//

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash_table_to_query_response_converter.h"

#include "internal_to_proto_aggregate_converters.h"
#include "stdbool.h"

PartialResult* convert_value_to_partial_result(HashTableValue value, ErrorInfo* err);
Value* convert_entry_to_value(const HashTableEntry* entry, ErrorInfo* err);
void free_values(Value** values, int count);
void free_value(Value* value);

// TODO check for memory leaks

QueryResponse* convert_hash_table_to_query_response(const HashTable* table, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    if (table == NULL || table->table == NULL)
    {
        LOG_INTERNAL_ERR("Invalid or uninitialized hash table");
        SET_ERR(err, INTERNAL_ERROR, "Invalid or uninitialized hash table", "Hash table or its table is NULL");
        return NULL;
    }

    QueryResponse* query_response = malloc(sizeof(QueryResponse));
    if (query_response == NULL)
    {
        LOG_ERR("Failed to allocate for query response while converting");
        SET_ERR(err, errno, "Failed to allocate for query response while converting", strerror(errno));
        return NULL;
    }
    query_response__init(query_response);

    const int values_counter = table->entries_count;
    int index_of_non_empty = 0;
    HashTableEntry* entry = table->table[index_of_non_empty];

    while (index_of_non_empty + 1 <= table->size && entry == NULL)
    {
        index_of_non_empty++;
        entry = table->table[index_of_non_empty];
    }

    if (index_of_non_empty == table->size)
    {
        LOG_INTERNAL_ERR("No entries to convert to query response");
        free(query_response);
        return NULL;
    }

    query_response->n_values = values_counter;
    query_response->values = malloc(sizeof(Value*) * query_response->n_values);
    if (query_response->values == NULL)
    {
        LOG_ERR("Failed to allocate for query response values while converting");
        SET_ERR(err, errno, "Failed to allocate for query response values while converting", strerror(errno));
        //query_response->n_values = 0;
        free(query_response);
        return NULL;
    }

    int converted_values = 0;
    for (int i = index_of_non_empty; i < table->size; i++)
    {
        HashTableEntry* current = table->table[i];
        while (current != NULL)
        {
            if (converted_values < query_response->n_values)
            {
                query_response->values[converted_values] = convert_entry_to_value(current, err);
                if (err->error_code != NO_ERROR)
                {
                    LOG_INTERNAL_ERR("Failed to convert query response values");
                    SET_ERR(err, INTERNAL_ERROR, "Failed to convert query response values", "");
                    //query_response->n_values = 0;
                    free_values(query_response->values, converted_values);
                    free(query_response);
                    return NULL;
                }
                converted_values++;
                current = current->next;
            }
            else
            {
                LOG_INTERNAL_ERR("Too many values to convert");
                break;
            }
        }
    }

    if (converted_values != values_counter)
    {
        LOG_INTERNAL_ERR("Wrong number of values converted to query response");
        free_values(query_response->values, converted_values);
        free(query_response);
        return NULL;
    }

    return query_response;
}

Value* convert_entry_to_value(const HashTableEntry* entry, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    Value* value = malloc(sizeof(Value));
    if (value == NULL)
    {
        LOG_ERR("Failed to allocate for query response value");
        SET_ERR(err, errno, "Failed to allocate for query response value", strerror(errno));
        return NULL;
    }
    value__init(value);

    value->n_results = entry->n_values;
    value->grouping_value = strdup(entry->key);
    if (value->grouping_value == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping_value");
        SET_ERR(err, errno, "Failed to allocate memory for grouping_value", strerror(errno));
        free_value(value); // Use the cleanup function here
        return NULL;
    }

    value->results = malloc(sizeof(PartialResult*) * value->n_results);
    if (value->results == NULL)
    {
        LOG_ERR("Failed to allocate for query response value results");
        SET_ERR(err, errno, "Failed to allocate for query response value results", strerror(errno));
        free_value(value); // Free all previously allocated resources
        return NULL;
    }

    for (int i = 0; i < entry->n_values; i++)
    {
        value->results[i] = convert_value_to_partial_result(entry->values[i], err);
        if (err->error_code != NO_ERROR)
        {
            free_value(value);
            return NULL;
        }
    }

    return value;
}

PartialResult* convert_value_to_partial_result(const HashTableValue value, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    PartialResult* result = malloc(sizeof(PartialResult));
    if (result == NULL)
    {
        LOG_ERR("Failed to allocate for query response value partial result");
        SET_ERR(err, errno, "Failed to allocate for query response value partial result", strerror(errno));
        return NULL;
    }

    partial_result__init(result);

    if (true == value.is_null)
    {
        result->is_null = true;
        result->count = 0;
        result->value_case = PARTIAL_RESULT__VALUE__NOT_SET;
        result->type = RESULT_TYPE__UNKNOWN;
        result->function = convert_aggregate(value.aggregate_function, err);
        return result;
    }

    result->is_null = false;
    result->count = value.count;
    result->function = convert_aggregate(value.aggregate_function, err);

    switch (value.type) {
        case HASH_TABLE_INT:
            result->type = RESULT_TYPE__INT;
            result->int_value = value.value;
            result->value_case = PARTIAL_RESULT__VALUE_INT_VALUE;
            break;
        case HASH_TABLE_FLOAT:
            result->type = RESULT_TYPE__FLOAT;
            result->float_value = value.float_value;
            result->value_case = PARTIAL_RESULT__VALUE_FLOAT_VALUE;
            break;
        case HASH_TABLE_DOUBLE:
            result->type = RESULT_TYPE__DOUBLE;
            result->double_value = value.double_value;
            result->value_case = PARTIAL_RESULT__VALUE_DOUBLE_VALUE;
            break;
        default:
            LOG_INTERNAL_ERR("Unsupported hash table type");
            break;
    }

    return result;
}

void free_values(Value** values, const int count)
{
    if (values == NULL)
    {
        return;
    }

    for (int i = 0; i < count; i++)
    {
        if (values[i] != NULL)
        {
            free_value(values[i]);
        }
    }
    free(values);
}


void free_value(Value* value)
{
    if (value == NULL)
    {
        return;
    }
    if (value->grouping_value != NULL)
    {
        free(value->grouping_value);
        value->grouping_value = NULL;
    }

    if (value->results != NULL)
    {
        for (int i = 0; i < value->n_results; i++)
        {
            if (value->results[i] != NULL)
            {
                free(value->results[i]);
                value->results[i] = NULL;
            }
        }
        free(value->results);
        value->results = NULL;
    }
    free(value);
    value = NULL;
}


QueryResponse* convert_hash_table_to_query_response_optimized(const HashTable* table, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    QueryResponse* query_response = malloc(sizeof(QueryResponse));
    if (query_response == NULL)
    {
        LOG_ERR("Failed to allocate memory for query response");
        SET_ERR(err, errno, "Failed to allocate memory for query response", strerror(errno));
        return NULL;
    }
    query_response__init(query_response);

    if (table == NULL || table->table == NULL)
    {
        LOG_ERR("Invalid or uninitialized hash table");
        SET_ERR(err, INTERNAL_ERROR, "Invalid or uninitialized hash table", "Hash table or its table is NULL");
        free(query_response);
        return NULL;
    }

    query_response->n_values = table->entries_count;
    query_response->values = malloc(sizeof(Value*) * query_response->n_values);
    if (query_response->values == NULL)
    {
        LOG_ERR("Failed to allocate memory for query response values");
        SET_ERR(err, errno, "Failed to allocate memory for query response values", strerror(errno));
        free(query_response);
        return NULL;
    }

    int converted_values = 0;

    for (int i = 0; i < table->size; i++)
    {
        HashTableEntry* entry = table->table[i];
        if (entry != NULL && !entry->is_deleted)
        {
            query_response->values[converted_values] = convert_entry_to_value(entry, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_ERR("Error converting hash table entry to query response value");
                free_values(query_response->values, converted_values);
                free(query_response);
                return NULL;
            }
            converted_values++;
        }
    }

    if (converted_values != query_response->n_values)
    {
        LOG_INTERNAL_ERR("Number of converted values does not match expected count");
        free_values(query_response->values, converted_values);
        free(query_response);
        return NULL;
    }

    return query_response;
}
