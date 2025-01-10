//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_PROTOCOL_H
#define REQUEST_PROTOCOL_H

#include "error_handling.h"
#include "hash_table.h"
#include "hash_table_interface.h"
#include "query_request.pb-c.h"
#include "query_response.pb-c.h"

QueryRequest* parse_incoming_request(int client_fd, ErrorInfo* err);
QueryResponse* parse_query_response(int client_fd, ErrorInfo* err);
void send_response(int client_fd, const QueryResponse* response, ErrorInfo* err);
void prepare_and_send_response(int client_fd, const char* guid, HashTableInterface* hash_table_interface,
                               const HashTable* ht, ErrorInfo* err);
void prepare_and_send_failure_response(int client_fd, const char* guid, ErrorInfo* err);
void print_query_response(const QueryResponse* query_response);
void prepare_and_send_result(const int client_fd, const char* guid, HashTableInterface* ht_interface,
                               const HashTable* ht, ErrorInfo* err);

#endif //REQUEST_PROTOCOL_H
