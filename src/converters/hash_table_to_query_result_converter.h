//
// Created by karol on 08.01.25.
//

#ifndef HASH_TABLE_TO_QUERY_RESULT_CONVERTER_H
#define HASH_TABLE_TO_QUERY_RESULT_CONVERTER_H
#include "hash_table_struct.h"
#include "query_result.pb-c.h"
#include "error_handling.h"

QueryResult* convert_hash_table_optimized_to_query_result(const HashTable* table, ErrorInfo* err);

#endif //HASH_TABLE_TO_QUERY_RESULT_CONVERTER_H
