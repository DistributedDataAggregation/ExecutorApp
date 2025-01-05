//
// Created by karol on 15.11.24.
//

#ifndef HASH_TABLE_TO_QUERY_RESPONSE_CONVERTER_H
#define HASH_TABLE_TO_QUERY_RESPONSE_CONVERTER_H

#include "error_handling.h"
#include "hash_table.h"
#include "query_response.pb-c.h"

QueryResponse* convert_hash_table_to_query_response(const HashTable* table, ErrorInfo* err);

QueryResponse* convert_hash_table_to_query_response_optimized(const HashTable* table, ErrorInfo* err);

#endif //HASH_TABLE_TO_QUERY_RESPONSE_CONVERTER_H
