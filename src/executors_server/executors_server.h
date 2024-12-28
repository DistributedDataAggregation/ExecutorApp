//
// Created by weronika on 12/17/24.
//

#ifndef EXECUTORS_SERVER_H
#define EXECUTORS_SERVER_H

#include <netinet/in.h>
#include "error_handling.h"

typedef struct {
    char ip_address[INET_ADDRSTRLEN];
    int socket;
} MainExecutorSocket;

typedef struct {
    MainExecutorSocket* sockets;
    size_t count;
    size_t capacity;
} MainExecutorsSockets;

void executors_server_init_main_executors_sockets(MainExecutorsSockets* sockets, size_t initial_capacity, ErrorInfo* err);
int executors_server_find_or_add_main_socket(MainExecutorsSockets* sockets, const char* ip_address, int port, ErrorInfo* err);
void executors_server_free(MainExecutorsSockets* sockets);

#endif //EXECUTORS_SERVER_H
