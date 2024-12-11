//
// Created by karol on 15.11.24.
//

#ifndef HASH_TABLE_TO_QUERY_RESPONSE_CONVERTER_H
#define HASH_TABLE_TO_QUERY_RESPONSE_CONVERTER_H

#include "hash_table.h"
#include "query_response.pb-c.h"

QueryResponse* convert_hash_table_to_query_response(HashTable *table);

#endif //HASH_TABLE_TO_QUERY_RESULT_CONVERTER_H
