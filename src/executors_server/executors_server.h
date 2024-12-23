//
// Created by weronika on 12/17/24.
//

#ifndef EXECUTORS_SERVER_H
#define EXECUTORS_SERVER_H
#include <stddef.h>
#include <netinet/in.h>

typedef struct {
    char ip_address[INET_ADDRSTRLEN];
    int socket;
} MainExecutorSocket;

typedef struct {
    MainExecutorSocket* sockets;
    size_t count;
    size_t capacity;
} MainExecutorsSockets;

void executors_server_init_main_executors_sockets(MainExecutorsSockets* sockets, size_t capacity);
int executors_server_find_or_add_main_socket(MainExecutorsSockets* sockets, const char* ip_address, const int port);
void executors_server_free_main_executors_sockets(MainExecutorsSockets* sockets);

#endif //EXECUTORS_SERVER_H
