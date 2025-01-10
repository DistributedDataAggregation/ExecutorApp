//
// Created by karol on 08.01.25.
//

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash_table_to_query_result_converter.h"
#include "internal_to_proto_aggregate_converters.h"

void free_result_values(ResultValue** values, int converted_values);
void free_result_value(ResultValue* value);
ResultValue* convert_entry(const HashTableEntry* entry, ErrorInfo* err);
CombinedResult* convert_value(HashTableValue value, ErrorInfo* err);
double get_average_for(HashTableValue value, ErrorInfo* err) ;

QueryResult* convert_hash_table_optimized_to_query_result(const HashTable* table, ErrorInfo* err) {
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

    QueryResult* query_result = (QueryResult*) malloc(sizeof(QueryResult));
    if (query_result == NULL)
    {
        LOG_ERR("Failed to allocate for query response while converting");
        SET_ERR(err, errno, "Failed to allocate for query response while converting", strerror(errno));
        return NULL;
    }

    query_result__init(query_result);

    query_result->n_values = table->entries_count;
    query_result->values = malloc(sizeof(ResultValue*) * query_result->n_values);
    if (query_result->values == NULL)
    {
        LOG_ERR("Failed to allocate memory for query response values");
        SET_ERR(err, errno, "Failed to allocate memory for query response values", strerror(errno));
        free(query_result);
        return NULL;
    }

    int converted_values = 0;

    for (int i = 0; i < table->size; i++)
    {
        HashTableEntry* entry = table->table[i];
        if (entry != NULL && !entry->is_deleted)
        {
            query_result->values[converted_values] = convert_entry(entry, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_INTERNAL_ERR("Error converting hash table entry to query response value");
                free_result_values(query_result->values, converted_values);
                free(query_result);
                query_result = NULL;
                return NULL;
            }
            converted_values++;
        }
    }

    return query_result;
}

ResultValue* convert_entry(const HashTableEntry* entry, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    ResultValue* value = malloc(sizeof(ResultValue));
    if (value == NULL)
    {
        LOG_ERR("Failed to allocate for query response value");
        SET_ERR(err, errno, "Failed to allocate for query response value", strerror(errno));
        return NULL;
    }

    result_value__init(value);

    value->n_results = entry->n_values;
    value->grouping_value = strdup(entry->key);
    if (value->grouping_value == NULL)
    {
        LOG_ERR("Failed to allocate memory for grouping_value");
        SET_ERR(err, errno, "Failed to allocate memory for grouping_value", strerror(errno));
        free_result_value(value); // Use the cleanup function here
        return NULL;
    }

    value->results = malloc(sizeof(CombinedResult*) * value->n_results);
    if (value->results == NULL)
    {
        LOG_ERR("Failed to allocate for query response value results");
        SET_ERR(err, errno, "Failed to allocate for query response value results", strerror(errno));
        free_result_value(value); // Free all previously allocated resources
        return NULL;
    }

    for (int i = 0; i < entry->n_values; i++)
    {
        value->results[i] = convert_value(entry->values[i], err);
        if (err->error_code != NO_ERROR)
        {
            free_result_value(value);
            return NULL;
        }
    }

    return value;
}

CombinedResult* convert_value(const HashTableValue value, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    CombinedResult* result = malloc(sizeof(CombinedResult));
    if (result == NULL)
    {
        LOG_ERR("Failed to allocate for query response value partial result");
        SET_ERR(err, errno, "Failed to allocate for query response value partial result", strerror(errno));
        return NULL;
    }

    combined_result__init(result);

    if (true == value.is_null)
    {
        result->is_null = true;
        result->value_case = COMBINED_RESULT__VALUE__NOT_SET;
        result->type = RESULT_TYPE__UNKNOWN;
        result->function =  convert_aggregate(value.aggregate_function, err);
        return result;
    }
    result->is_null = false;
    result->function = convert_aggregate(value.aggregate_function, err);

    if (result->function == AGGREGATE__Average) {
        result->type = RESULT_TYPE__DOUBLE;
        result->double_value = get_average_for(value, err);
        if (err->error_code != NO_ERROR) {
            free(result);
            return NULL;
        }
        result->value_case = COMBINED_RESULT__VALUE_DOUBLE_VALUE;

        // early return just for average
        return result;
    }

    switch (value.type) {
        case HASH_TABLE_INT:
            result->type = RESULT_TYPE__INT;
            result->int_value = value.value;
            result->value_case = COMBINED_RESULT__VALUE_INT_VALUE;
        break;
        case HASH_TABLE_FLOAT:
            result->type = RESULT_TYPE__FLOAT;
        result->float_value = value.float_value;
        result->value_case = COMBINED_RESULT__VALUE_FLOAT_VALUE;
        break;
        case HASH_TABLE_DOUBLE:
            result->type = RESULT_TYPE__DOUBLE;
            result->double_value = value.double_value;
            result->value_case = COMBINED_RESULT__VALUE_DOUBLE_VALUE;
        break;
        default:
            LOG_INTERNAL_ERR("Unsupported hash table type");
        break;
    }

    return result;
}

double get_average_for(const HashTableValue value, ErrorInfo* err) {
    switch (value.type) {
        case HASH_TABLE_INT:
            return ((double)value.value)/(double)value.count;
        case HASH_TABLE_FLOAT:
            return value.float_value/(double)value.count;
        case HASH_TABLE_DOUBLE:
            return value.double_value/(double)value.count;
        default:
            LOG_INTERNAL_ERR("Unsupported hash table type");
            SET_ERR(err, INTERNAL_ERROR, "Calculating average on unsupported data type", "INT, FLOAT and DOUBLE are supported");
            return 0.0;
    }
}


void free_result_values(ResultValue** values, const int converted_values) {
    if (values == NULL)
        return;

    for (int i = 0; i < converted_values; i++) {
        if (values[i] != NULL) {
            free_result_value(values[i]);
            values[i] = NULL;
        }
    }

    free(values);
    values = NULL;
}

void free_result_value(ResultValue* value) {
    if (value == NULL) {
        return;
    }

    if (value->grouping_value != NULL) {
        free(value->grouping_value);
        value->grouping_value = NULL;
    }

    if (value->results != NULL) {
        for (int i=0;i<value->n_results;i++) {
            if (value->results[i] != NULL) {
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