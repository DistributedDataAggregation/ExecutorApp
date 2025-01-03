//
// Created by weronika on 12/17/24.
//

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "boolean.h"
#include "error_handling.h"
#include "executors_server.h"
#include "logging.h"
#include "socket_utilities.h"

void executors_server_init_main_executors_sockets(MainExecutorsSockets* sockets,
                                                  const size_t initial_capacity, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }

    sockets->sockets = malloc(sizeof(MainExecutorSocket) * initial_capacity);
    if (sockets->sockets == NULL)
    {
        LOG_ERR("Failed to allocate memory for main executors sockets array");
        SET_ERR(err, errno, "Failed to allocate memory for main executors sockets array", strerror(errno));
        return;
    }

    sockets->count = 0;
    sockets->capacity = initial_capacity;
}

void executors_server_free(MainExecutorsSockets* sockets)
{
    for (size_t i = 0; i < sockets->count; i++)
    {
        close(sockets->sockets[i].socket);
    }
    free(sockets->sockets);
    sockets->sockets = NULL;
    sockets->count = 0;
    sockets->capacity = 0;
}

int executors_server_find_or_add_main_socket(MainExecutorsSockets* sockets, const char* ip_address,
                                             const int port, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return -1;
    }

    for (size_t i = 0; i < sockets->count; i++)
    {
        if (strcmp(sockets->sockets[i].ip_address, ip_address) == 0)
        {
            LOG("Found main socket %s\n", sockets->sockets[i].ip_address);
            return sockets->sockets[i].socket;
        }
    }

    if (sockets->count >= sockets->capacity)
    {
        sockets->capacity *= 2;
        sockets->sockets = realloc(sockets->sockets, sizeof(MainExecutorSocket) * sockets->capacity);
        if (sockets->sockets == NULL)
        {
            LOG_ERR("Failed to reallocate memory for main executors sockets array");
            SET_ERR(err, errno, "Failed to reallocate memory for main executors sockets array", strerror(errno));
            return -1;
        }
    }

    const int new_socket = create_tcp_socket("0.0.0.0", TRUE, FALSE, 0, err);
    if (err->error_code != NO_ERROR)
    {
        return -1;
    }

    //ip_address = "0.0.0.0";
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(port)
    };

    if (inet_aton(ip_address, &address.sin_addr) == 0)
    {
        close(new_socket);
        LOG_ERR("Failed to convert ip address");
        SET_ERR(err, errno, "Failed to convert ip address", strerror(errno));
        return -1;
    }

    if (connect(new_socket, (struct sockaddr*)&address, sizeof(address)) < 0)
    {
        close(new_socket);
        LOG_ERR("Failed to connect to main socket");
        SET_ERR(err, errno, "Failed to connect to main socket", strerror(errno));
        return -1;
    }

    strncpy(sockets->sockets[sockets->count].ip_address, ip_address, INET_ADDRSTRLEN);
    sockets->sockets[sockets->count].socket = new_socket;
    sockets->count++;

    LOG("Added new main socket %d to main_executors\n", new_socket);
    return new_socket;
}

void executors_server_remove_main_socket(MainExecutorsSockets* sockets, const int socket)
{
    int index = -1;
    for (int i = 0; i < sockets->count; i++)
    {
        if (sockets->sockets[i].socket == socket)
        {
            index = i;
            break;
        }
    }
    if (index == -1)
        return;

    close(sockets->sockets[index].socket);
    sockets->sockets[index].socket = sockets->sockets[sockets->count - 1].socket;
    strncpy(sockets->sockets[index].ip_address, sockets->sockets[sockets->count - 1].ip_address, INET_ADDRSTRLEN);
    sockets->count--;
}
