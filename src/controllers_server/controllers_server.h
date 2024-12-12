//
// Created by weronika on 11/23/24.
//

#ifndef CONTROLLER_SERVER_H
#define CONTROLLER_SERVER_H

#include <stdlib.h>

typedef struct {
    int* clients;
    size_t count;
    size_t capacity;
} ClientArray;

void controller_server_init_client_array(ClientArray* array, const size_t initial_capacity);

void controller_server_add_client(ClientArray* array, int client_fd);

void controller_server_accept_clients(ClientArray* array, const int socket_fd, const fd_set* read_fds);

void controller_server_set_clients(const ClientArray* array, int* max_fd, fd_set* read_fds);

void controller_server_remove_client(ClientArray* array, const size_t index);

void controller_server_free_client_array(ClientArray* array);

#endif //CONTROLLER_SERVER_H
