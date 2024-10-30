//
// Created by karol on 27.10.24.
//

#include "request_protocol.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <netinet/in.h>

#include "error_utilites.h"

QueryRequest* parse_incoming_request(int client_socket) {

    uint32_t message_size;
    if (read(client_socket, &message_size, sizeof(message_size)) <= 0) {
        ERR_AND_EXIT("read");
    }
    message_size = ntohl(message_size);

    printf("Message size: %d\n", message_size);

    uint8_t* buffer = (uint8_t*)malloc(message_size*sizeof(uint8_t));
    ssize_t bytes = 0;
    ssize_t total_bytes_read = 0;

    while(total_bytes_read < message_size) {
        bytes = read(client_socket, buffer+total_bytes_read, message_size-total_bytes_read);
        if (bytes < 0) {
            ERR_AND_EXIT("read");
        }
        total_bytes_read += bytes;
    }

    QueryRequest* request = query_request__unpack(NULL, message_size, buffer);
    if(request == NULL) {
        ERR_AND_EXIT("query_request__unpack");
    }

    return request;
}
