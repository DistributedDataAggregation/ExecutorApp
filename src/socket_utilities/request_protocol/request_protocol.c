//
// Created by karol on 27.10.24.
//

#include "request_protocol.h"

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <asm-generic/errno.h>
#include <netinet/in.h>

#include "error_utilites.h"
#include "hash_table_to_query_response_converter.h"
#include "query_response.pb-c.h"

uint8_t* get_packed_protobuffer(int client_socket, uint32_t* message_size);

void print_partial_result(const PartialResult *partial_result) {
    if (!partial_result) return;
    printf("    PartialResult:\n");
    printf("      Value: %lld\n", (long long)partial_result->value);
    printf("      Count: %lld\n", (long long)partial_result->count);
}

// Function to print Value
void print_value(const Value *value) {
    if (!value) return;
    printf("  Value:\n");
    printf("    Grouping Value: %s\n", value->grouping_value ? value->grouping_value : "(null)");
    printf("    Number of Results: %zu\n", value->n_results);

    for (size_t i = 0; i < value->n_results; ++i) {
        if (value->results[i]) {
            print_partial_result(value->results[i]);
        }
    }
}

// Function to print QueryResponse
void print_query_response(const QueryResponse *query_response) {
    if (!query_response) {
        printf("QueryResponse is null\n");
        return;
    }

    printf("QueryResponse:\n");
    printf("  Number of Values: %zu\n", query_response->n_values);

    for (size_t i = 0; i < query_response->n_values; ++i) {
        if (query_response->values[i]) {
            print_value(query_response->values[i]);
        }
    }
}

QueryRequest* parse_incoming_request(int client_socket)
{
    uint32_t message_size;
    uint8_t* buffer = get_packed_protobuffer(client_socket, &message_size);

    QueryRequest* request = query_request__unpack(NULL, message_size, buffer);
    if(request == NULL) {
        ERR_AND_EXIT("query_request__unpack");
    }

    return request;
}

void send_reponse(int clientfd, HashTable* ht) {
    QueryResponse* response = convert_hash_table_to_query_response(ht);
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

QueryResponse* parse_query_response(int client_socket)
{
    uint32_t message_size;
    uint8_t* buffer = get_packed_protobuffer(client_socket, &message_size);

    QueryResponse* request = query_response__unpack(NULL, message_size, buffer);
    if(request == NULL) {
        ERR_AND_EXIT("query_request__unpack");
    }

    return request;
}

uint8_t* get_packed_protobuffer(int client_socket, uint32_t* message_size)
{
    if (read(client_socket, message_size, sizeof(uint32_t)) <= 0) {
        REPORT_ERR("read");
    }
    (*message_size) = ntohl((*message_size));

    printf("Message size: %d\n", (*message_size));

    uint8_t* buffer = (uint8_t*)malloc((*message_size)*sizeof(uint8_t));
    ssize_t bytes = 0;
    ssize_t total_bytes_read = 0;

    while(total_bytes_read < (*message_size)) {
        bytes = read(client_socket, buffer+total_bytes_read, (*message_size)-total_bytes_read);
        if (bytes < 0) {
            ERR_AND_EXIT("read");
        }
        total_bytes_read += bytes;
    }

    return buffer;
}



