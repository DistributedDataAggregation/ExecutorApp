//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_PROTOCOL_H
#define REQUEST_PROTOCOL_H

#include "hash_table.h"
#include "query_request.pb-c.h"
#include "query_response.pb-c.h"

QueryRequest* parse_incoming_request(int client_socket);
QueryResponse* parse_query_response(int client_socket);
void send_reponse(int clientfd, HashTable* ht) ;
void print_query_response(const QueryResponse *query_response);

#endif //REQUEST_PROTOCOL_H
