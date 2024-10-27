//
// Created by karol on 27.10.24.
//

#ifndef REQUEST_PROTOCOL_H
#define REQUEST_PROTOCOL_H

#include "request_protocol/request.h"

int parse_incoming_request(int client_socket, Request* request);

#endif //REQUEST_PROTOCOL_H
