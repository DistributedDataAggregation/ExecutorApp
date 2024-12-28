//
// Created by weronika on 12/17/24.
//

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "boolean.h"
#include "error_handling.h"
#include "executors_server.h"
#include "socket_utilities.h"

void executors_server_init_main_executors_sockets(MainExecutorsSockets* sockets, const size_t initial_capacity) {
    sockets->sockets = malloc(sizeof(MainExecutorSocket) * initial_capacity);
    if (sockets->sockets == NULL) {
        LOG_ERR_AND_EXIT("Failed to allocate memory for main executors sockets array");
    }

    sockets->count = 0;
    sockets->capacity = initial_capacity;
}

void executors_server_free(MainExecutorsSockets* sockets) {
    for (size_t i = 0; i < sockets->count; i++) {
        close(sockets->sockets[i].socket);
    }
    free(sockets->sockets);
    sockets->sockets = NULL;
    sockets->count = 0;
    sockets->capacity = 0;
}

int executors_server_find_or_add_main_socket(MainExecutorsSockets* sockets, const char* ip_address, const int port) {

    for (size_t i = 0; i < sockets->count; i++) {
        if (strcmp(sockets->sockets[i].ip_address, ip_address) == 0) {
            printf("Found main socket %s\n", sockets->sockets[i].ip_address);
            return sockets->sockets[i].socket;
        }
    }

    if (sockets->count >= sockets->capacity) {
        sockets->capacity *= 2;
        sockets->sockets = realloc(sockets->sockets, sizeof(MainExecutorSocket) * sockets->capacity);
        if (sockets->sockets == NULL) {
            LOG_ERR_AND_EXIT("Failed to reallocate memory for main socket array");
        }
    }

    const int new_socket = create_tcp_socket("0.0.0.0", TRUE, FALSE, 0);
    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port)
    };

    if (inet_pton(AF_INET, ip_address, &server_addr.sin_addr) <= 0) {
        close(new_socket);
        LOG_ERR_AND_EXIT("Failed to convert ip address '%s'", ip_address);
    }

    if (connect(new_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        close(new_socket);
        LOG_ERR_AND_EXIT("Failed to connect to main socket '%s'", ip_address);
    }

    strncpy(sockets->sockets[sockets->count].ip_address, ip_address, INET_ADDRSTRLEN);
    sockets->sockets[sockets->count].socket = new_socket;
    sockets->count++;

    printf("Added new main socket to main_executors\n");
    return new_socket;
}
