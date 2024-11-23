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
#include "hash_table_to_query_response_converter.h"
#include "query_response.pb-c.h"

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

void send_reponse(int clientfd, HashTable* ht) {
    QueryResponse* response = convert_table(ht);

    ssize_t size = query_response__get_packed_size(response);
    uint8_t* buffer = (uint8_t*)malloc(sizeof(uint8_t)*size);
    if (buffer == NULL) {
        ERR_AND_EXIT("malloc");
    }
    memset(buffer, 0, size);

    int size_to_send = htonl(size);
    if(write(clientfd, &size_to_send, sizeof(size_to_send)) <= 0) {
        ERR_AND_EXIT("send");
    }

    int stored = query_response__pack(response, buffer);
    if(stored != size) {
        ERR_AND_EXIT("results__pack");
    }

    if(send(clientfd, buffer, size, 0) != size) {
        ERR_AND_EXIT("send");
    }

    query_response__free_unpacked(response, NULL);
}