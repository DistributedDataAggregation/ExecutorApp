//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_PROTOCOL_H
#define REQUEST_PROTOCOL_H

#include "request_protocol/request.h"
#include "query_request/query_request.pb-c.h"

QueryRequest* parse_incoming_request(int client_socket);

#endif //REQUEST_PROTOCOL_H
