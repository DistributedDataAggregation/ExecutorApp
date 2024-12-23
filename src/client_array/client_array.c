//
// Created by weronika on 11/23/24.
//
#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/select.h>
#include <sys/socket.h>
#include "boolean.h"

#include "client_array.h"
#include "socket_utilities.h"
#include "error_utilites.h"

void client_array_init(ClientArray* array, const size_t initial_capacity) {
    array->clients = malloc(initial_capacity * sizeof(int));
    array->count = 0;
    array->capacity = initial_capacity;
}

void client_array_add_client(ClientArray* array, int client_fd) {
    if (array->count == array->capacity) {
        array->capacity *= 2;
        array->clients = realloc(array->clients, array->capacity * sizeof(int));
        if (!array->clients) {
            ERR_AND_EXIT("realloc failed");
        }
    }
    array->clients[array->count++] = client_fd;
}

void client_array_accept_clients(ClientArray* array, const int socket_fd, const fd_set* read_fds) {
    if (FD_ISSET(socket_fd, read_fds)) {
        int client_fd;
        while ((client_fd = accept_client(socket_fd, TRUE)) != -1) {
            printf("Client connected: %d\n", client_fd);
            client_array_add_client(array, client_fd);
        }
    }
}

void client_array_set_clients(const ClientArray* array, int* max_fd, fd_set* read_fds) {
    for (size_t i = 0; i < array->count; i++) {
        FD_SET(array->clients[i], read_fds);
        if (array->clients[i] > *max_fd) {
            *max_fd = array->clients[i];
        }
    }
}

void client_array_remove_client(ClientArray* array, const size_t index) {
    if (index >= array->count) return;
    close(array->clients[index]);
    array->clients[index] = array->clients[array->count - 1];
    array->count--;
}

void client_array_free(ClientArray* array) {
    for (size_t i = 0; i < array->count; i++) {
        close(array->clients[i]);
    }
    free(array->clients);
    array->clients = NULL;
    array->count = 0;
    array->capacity = 0;
}
