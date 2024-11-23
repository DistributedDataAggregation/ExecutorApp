//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_PROTOCOL_H
#define REQUEST_PROTOCOL_H

#include "hash_table.h"
#include "query_request.pb-c.h"

QueryRequest* parse_incoming_request(int client_socket);
void send_reponse(int clientfd, HashTable* ht) ;

#endif //REQUEST_PROTOCOL_H
