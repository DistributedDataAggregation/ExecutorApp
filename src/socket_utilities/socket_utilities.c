//
// Created by karol on 27.10.24.
//
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include "boolean.h"
#include "error_handling.h"
#include "socket_utilities.h"

#define CONNECTIONS_MAX 3

void set_socket_to_nonblocking(int socket_fd);
void set_socket_to_reuse(int socket_fd);

int create_and_listen_on_tcp_socket(char* address_string, const int so_reuse,
        const int non_blocking, const int port) {

    const int socket_fd = create_tcp_socket(address_string, so_reuse, non_blocking, port);

    if(listen(socket_fd, CONNECTIONS_MAX) == -1) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to listen on a socket");
    }

    return socket_fd;
}

int create_tcp_socket(const char* address_string, const int so_reuse, const int non_blocking, const int port) {

    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        LOG_ERR_AND_EXIT("Failed to create socket");
    }

    if (setsockopt(socket_fd, SOL_SOCKET, TCP_NODELAY, (int[]){1}, sizeof(int)) == -1) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to set TCP_NODELAY on a socket");
    }

    if (non_blocking == TRUE) {
        set_socket_to_nonblocking(socket_fd);
    }

    if(so_reuse == TRUE) {
        set_socket_to_reuse(socket_fd);
    }

    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(port)
    };

    if (inet_aton(address_string, &address.sin_addr) == 0) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Invalid IP address format");
    }

    if (bind(socket_fd, (struct sockaddr*)&address, sizeof(address)) == -1) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to bind socket");
    }

    return socket_fd;
}

void set_socket_to_reuse(const int socket_fd) {
    const int opt = 1;
    if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to set SO_REUSEADDR on a socket");
    }
}

void set_socket_to_nonblocking(const int socket_fd) {

    const int flags = fcntl(socket_fd, F_GETFL, 0);
    if (flags == -1) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to get socket flags");
    }

    if (fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(socket_fd);
        LOG_ERR_AND_EXIT("Failed to set O_NONBLOCK socket flag");
    }
}

int accept_client(const int socket_fd, const int is_non_blocking) {

    const int client_fd = accept(socket_fd, NULL, NULL);
    if(client_fd == -1) {
        if(is_non_blocking == TRUE && (errno == EWOULDBLOCK || errno == EAGAIN)) {
            printf("No pending client connection\n");
        } else {
            close(socket_fd);
            LOG_ERR_AND_EXIT("Failed to accept a client connection");
        }
    }

    return client_fd;
}

int get_port_from_env(const char* env_var) {
    const char* port_str = getenv(env_var);
    if(port_str == NULL) {
        LOG_ERR_AND_EXIT("Could not get port env variable");
    }

    const long int port = strtol(port_str, NULL, 10);
    if (errno != 0) {
        LOG_ERR_AND_EXIT("Could not get port number");
    }

    return (int)port;
}
