//
// Created by karol on 27.10.24.
//

#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "error_handling.h"
#include "hash_table_to_query_response_converter.h"
#include "query_response.pb-c.h"
#include "request_protocol.h"
#include "hash_table_interface.h"
#include "logging.h"

void get_message_size(int client_fd, uint32_t* message_size, ErrorInfo* err);
void get_packed_proto_buffer(int client_fd, uint32_t message_size, uint8_t* buffer, ErrorInfo* err);
void get_returned_error(Error* error, ErrorInfo* err);
void send_result(int client_fd, const QueryResult* result, ErrorInfo* err);
void prepare_and_send_failure_result(int client_fd, const char* guid, ErrorInfo* err) ;

void print_partial_result(const PartialResult* partial_result)
{
    if (!partial_result) return;
    printf("    PartialResult:\n");
    printf("      Value: %lld\n", (long long)partial_result->int_value);
    printf("      Count: %lld\n", (long long)partial_result->count);
}

void print_value(const Value* value)
{
    if (!value) return;
    printf("  Value:\n");
    printf("    Grouping Value: %s\n", value->grouping_value ? value->grouping_value : "(null)");
    printf("    Number of Results: %zu\n", value->n_results);

    for (size_t i = 0; i < value->n_results; ++i)
    {
        if (value->results[i])
        {
            print_partial_result(value->results[i]);
        }
    }
}

void print_query_response(const QueryResponse* query_response)
{
    if (!query_response)
    {
        printf("QueryResponse is null\n");
        return;
    }

    printf("QueryResponse:\n");
    printf("  Number of Values: %zu\n", query_response->n_values);

    for (size_t i = 0; i < query_response->n_values; ++i)
    {
        if (query_response->values[i])
        {
            print_value(query_response->values[i]);
        }
    }
}

QueryRequest* parse_incoming_request(const int client_fd, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    uint32_t message_size;
    get_message_size(client_fd, &message_size, err);
    if (err->error_code != NO_ERROR)
    {
        return NULL;
    }
    uint8_t* buffer = (uint8_t*)malloc((message_size) * sizeof(uint8_t));
    if (buffer == NULL)
    {
        LOG_ERR("Failed to allocate memory for message buffer");
        SET_ERR(err, errno, "Failed to allocate memory for message buffer", strerror(errno));
        return NULL;
    }
    get_packed_proto_buffer(client_fd, message_size, buffer, err);
    if (err->error_code != NO_ERROR)
    {
        free(buffer);
        return NULL;
    }

    QueryRequest* request = query_request__unpack(NULL, message_size, buffer);
    free(buffer);
    if (request == NULL)
    {
        LOG_ERR("Failed to unpack message buffer for query request");
        SET_ERR(err, errno, "Failed to unpack message buffer for query request", strerror(errno));
        return NULL;
    }

    return request;
}

void send_response(const int client_fd, const QueryResponse* response, ErrorInfo* err)
{
    const ssize_t size = (ssize_t)query_response__get_packed_size(response);
    uint8_t* buffer = (uint8_t*)malloc(sizeof(uint8_t) * size);
    if (buffer == NULL)
    {
        LOG_ERR("Failed to allocate memory for message buffer");
        SET_ERR(err, errno, "Failed to allocate memory for message buffer", strerror(errno));
        return;
    }
    memset(buffer, 0, size);

    const int size_to_send = (int)htonl(size);
    ssize_t bytes_written = write(client_fd, &size_to_send, sizeof(size_to_send));
    if (bytes_written <= 0)
    {
        LOG_ERR("Failed to send message size to client");
        SET_ERR(err, errno, "Failed to send message size to client", strerror(errno));
        free(buffer);
        return;
    }
    LOG("Sent %zd bytes for message size to client", bytes_written);

    const int stored = (int)query_response__pack(response, buffer);
    if (stored != size)
    {
        LOG_ERR("Failed to pack message buffer");
        SET_ERR(err, errno, "Failed to pack message buffer", strerror(errno));
        free(buffer);
        return;
    }
    LOG("Packed %d bytes into message buffer", stored);

    ssize_t bytes_sent = send(client_fd, buffer, size, 0);
    if (bytes_sent != size)
    {
        LOG("Failed to send message to client. Sent %zd %zd bytes", bytes_sent, size);
        LOG_ERR("Failed to send message to client");
        SET_ERR(err, errno, "Failed to send message to client", strerror(errno));
    }

    LOG("Successfully sent %zd bytes to client", bytes_sent);


    free(buffer);
}

void prepare_and_send_response(const int client_fd, const char* guid, HashTableInterface* ht_interface,
                               const HashTable* ht, ErrorInfo* err)
{
    if (err == NULL || err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_response(client_fd, guid, err);
        return;
    }

    QueryResponse* response = ht_interface->convert_to_response(ht, err);

    if (err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_response(client_fd, guid, err);
        query_response__free_unpacked(response, NULL);
        return;
    }

    response->guid = strdup(guid);

    send_response(client_fd, response, err);
    if (err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_response(client_fd, guid, err);
    }

    query_response__free_unpacked(response, NULL);
}

void prepare_and_send_result(const int client_fd, const char* guid, HashTableInterface* ht_interface,
                               const HashTable* ht, ErrorInfo* err)
{
    if (err == NULL || err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_response(client_fd, guid, err);
        return;
    }

    QueryResult* result = ht_interface->convert_to_result(ht, err);
    result->guid = strdup(guid);
    if (err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_result(client_fd, guid, err);
        query_result__free_unpacked(result, NULL);
        return;
    }

    send_result(client_fd, result, err);
    if (err->error_code != NO_ERROR)
    {
        prepare_and_send_failure_result(client_fd, guid, err);
    }

    query_result__free_unpacked(result, NULL);
}

void prepare_and_send_failure_response(const int client_fd, const char* guid, ErrorInfo* err)
{
    QueryResponse* response = malloc(sizeof(QueryResponse));
    if (response == NULL)
    {
        LOG_ERR("Failed to allocate memory for response");
        // TODO handle
        return;
    }

    query_response__init(response);
    response->guid = strdup(guid);
    response->error = malloc(sizeof(Error));
    if (response->error == NULL)
    {
        free(response);
        LOG_ERR("Failed to allocate memory for error in response");
        // TODO handle
        return;
    }

    get_returned_error(response->error, err);

    send_response(client_fd, response, err);
    query_response__free_unpacked(response, NULL);
}

void prepare_and_send_failure_result(const int client_fd, const char* guid, ErrorInfo* err) {
    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed in null err");
        return;
    }

    QueryResult* result = malloc(sizeof(QueryResult));
    query_result__init(result);

    result->guid = strdup(guid);
    result->error = malloc(sizeof(Error));
    get_returned_error(result->error, err);

    send_result(client_fd, result, err);
    query_result__free_unpacked(result, NULL);
}

void send_result(const int client_fd, const QueryResult* result, ErrorInfo* err) {
    const ssize_t size = (ssize_t)query_result__get_packed_size(result);
    uint8_t* buffer = (uint8_t*)malloc(sizeof(uint8_t) * size);
    if (buffer == NULL)
    {
        LOG_ERR("Failed to allocate memory for message buffer");
        SET_ERR(err, errno, "Failed to allocate memory for message buffer", strerror(errno));
        return;
    }
    memset(buffer, 0, size);

    const int size_to_send = (int)htonl(size);
    ssize_t bytes_written = write(client_fd, &size_to_send, sizeof(size_to_send));
    if (bytes_written <= 0)
    {
        LOG_ERR("Failed to send message size to client");
        SET_ERR(err, errno, "Failed to send message size to client", strerror(errno));
        free(buffer);
        return;
    }
    LOG("Sent %zd bytes for message size to client", bytes_written);

    const int stored = (int)query_result__pack(result, buffer);
    if (stored != size)
    {
        LOG_ERR("Failed to pack message buffer");
        SET_ERR(err, errno, "Failed to pack message buffer", strerror(errno));
        free(buffer);
        return;
    }
    LOG("Packed %d bytes into message buffer", stored);

    ssize_t bytes_sent = send(client_fd, buffer, size, 0);
    if (bytes_sent != size)
    {
        LOG("Failed to send message to client. Sent %zd %zd bytes", bytes_sent, size);
        LOG_ERR("Failed to send message to client");
        SET_ERR(err, errno, "Failed to send message to client", strerror(errno));
    }

    LOG("Successfully sent %zd bytes to client", bytes_sent);

    free(buffer);
}

void get_returned_error(Error* error, ErrorInfo* err) {

    if (error == NULL ) {
        LOG_INTERNAL_ERR("Passed in null error");
        return;
    }

    error__init(error);

    if (err != NULL)
    {
        error->message = malloc(sizeof(err->error_message) + 1);
        if (error->message == NULL)
        {
            free(error);
            LOG_ERR("Failed to allocate memory for error message in response");
            // TODO handle
            return;
        }
        error->inner_message = malloc(sizeof(err->inner_error_message) + 1);
        if (error->inner_message == NULL)
        {
            free(error->inner_message);
            free(error);
            LOG_ERR("Failed to allocate memory for inner error message in response");
            // TODO handle
            return;
        }
        strncpy(error->message, err->error_message, sizeof(err->error_message));
        strncpy(error->inner_message, err->inner_error_message, sizeof(err->inner_error_message));

        CLEAR_ERR(err);
    }

}

QueryResponse* parse_query_response(const int client_fd, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return NULL;
    }

    uint32_t message_size;
    get_message_size(client_fd, &message_size, err);
    if (err->error_code != NO_ERROR)
    {
        return NULL;
    }
    uint8_t* buffer = (uint8_t*)malloc((message_size) * sizeof(uint8_t));
    if (buffer == NULL)
    {
        LOG_ERR("Failed to allocate memory for message buffer");
        SET_ERR(err, errno, "Failed to allocate memory for message buffer", strerror(errno));
        return NULL;
    }
    get_packed_proto_buffer(client_fd, message_size, buffer, err);
    if (err->error_code != NO_ERROR)
    {
        free(buffer);
        return NULL;
    }

    QueryResponse* response = query_response__unpack(NULL, message_size, buffer);
    free(buffer);
    if (response == NULL)
    {
        LOG_ERR("Failed to unpack message buffer for query response");
        SET_ERR(err, errno, "Failed to unpack message buffer for query response", strerror(errno));
        return NULL;
    }

    return response;
}

void get_message_size(const int client_fd, uint32_t* message_size, ErrorInfo* err)
{
    const ssize_t bytes = read(client_fd, message_size, sizeof(uint32_t));
    if (bytes == 0)
    {
        LOG_INTERNAL_ERR("Failed reading message size: Attempted to read from a closed socket");
        SET_ERR(err, SOCKET_CLOSED, "Failed reading message size", "Attempted to read from a closed socket");
        return;
    }
    if (bytes < 0)
    {
        LOG_ERR("Failed reading message size");
        SET_ERR(err, errno, "Failed reading message size", strerror(errno));
        return;
    }
    (*message_size) = ntohl((*message_size));

    LOG("Message size: %d\n", (*message_size));
}

void get_packed_proto_buffer(const int client_fd, const uint32_t message_size, uint8_t* buffer, ErrorInfo* err)
{
    ssize_t bytes = 0;
    ssize_t total_bytes_read = 0;

    while (total_bytes_read < (message_size))
    {
        bytes = read(client_fd, buffer + total_bytes_read, (message_size) - total_bytes_read);
        if (bytes == 0)
        {
            LOG_INTERNAL_ERR("Failed reading message: Attempted to read from a closed socket");
            SET_ERR(err, SOCKET_CLOSED, "Failed reading message", "Attempted to read from a closed socket");
            return;
        }
        if (bytes < 0)
        {
            LOG_ERR("Failed reading message");
            SET_ERR(err, errno, "Failed reading message", strerror(errno));
            return;
        }
        total_bytes_read += bytes;
    }
}
