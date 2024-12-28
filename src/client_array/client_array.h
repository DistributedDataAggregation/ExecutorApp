//
// Created by weronika on 11/23/24.
//

#ifndef CLIENT_ARRAY_H
#define CLIENT_ARRAY_H

#include <stdlib.h>

typedef struct {
    int* clients;
    size_t count;
    size_t capacity;
} ClientArray;

void client_array_init(ClientArray* array, size_t initial_capacity);

void client_array_add_client(ClientArray* array, int client_fd);

void client_array_accept_clients(ClientArray* array, int socket_fd, const fd_set* read_fds);

void client_array_set_clients(const ClientArray* array, int* max_fd, fd_set* read_fds);

void client_array_remove_client(ClientArray* array, size_t index);

void client_array_free(ClientArray* array);

#endif //CLIENT_ARRAY_H
