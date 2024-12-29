//
// Created by karol on 27.10.24.
//
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "boolean.h"
#include "error_handling.h"
#include "socket_utilities.h"

#define CONNECTIONS_MAX 3

int create_and_listen_on_tcp_socket(const char* address_string, const int so_reuse,
        const int non_blocking, const int port, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return -1;
    }

    const int socket_fd = create_tcp_socket(address_string, so_reuse, non_blocking, port, err);
    if (err->error_code != NO_ERROR)
        return -1;

    if(listen(socket_fd, CONNECTIONS_MAX) == -1) {
        close(socket_fd);
        LOG_ERR("Failed to listen on a socket");
        SET_ERR(err, errno, "Failed to listen on a socket", strerror(errno));
        return -1;
    }

    return socket_fd;
}

int create_tcp_socket(const char* address_string, const int so_reuse, const int non_blocking,
        const int port, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return -1;
    }

    const int socket_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_fd < 0) {
        LOG_ERR("Failed to create socket");
        SET_ERR(err, errno, "Failed to create socket", strerror(errno));
        return -1;
    }

    if (setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, (int[]){1}, sizeof(int)) == -1) { // TODO is IPPROTO_TCP ok?
        close(socket_fd);
        LOG_ERR("Failed to set TCP_NODELAY on a socket");
        SET_ERR(err, errno, "Failed to set TCP_NODELAY on a socket", strerror(errno));
        return -1;
    }

    if (non_blocking == TRUE) {
        const int flags = fcntl(socket_fd, F_GETFL, 0);
        if (flags == -1) {
            close(socket_fd);
            LOG_ERR("Failed to get socket flags");
            SET_ERR(err, errno, "Failed to get socket flags", strerror(errno));
            return -1;
        }

        if (fcntl(socket_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
            close(socket_fd);
            LOG_ERR("Failed to set O_NONBLOCK socket flag");
            SET_ERR(err, errno, "Failed to set O_NONBLOCK socket flag", strerror(errno));
            return -1;
        }
    }

    if(so_reuse == TRUE) {
        const int opt = 1;
        if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            close(socket_fd);
            LOG_ERR("Failed to set SO_REUSEADDR on a socket");
            SET_ERR(err, errno, "Failed to set SO_REUSEADDR on a socket", strerror(errno));
            return -1;
        }
    }

    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(port)
    };

    if (inet_aton(address_string, &address.sin_addr) == 0) {
        close(socket_fd);
        LOG_ERR("Failed to convert ip address");
        SET_ERR(err, errno, "Failed to convert ip address", strerror(errno));
        return -1;
    }

    if (bind(socket_fd, (struct sockaddr*)&address, sizeof(address)) == -1) {
        close(socket_fd);
        LOG_ERR("Failed to bind socket");
        SET_ERR(err, errno, "Failed to bind socket", strerror(errno));
        return -1;
    }

    return socket_fd;
}

int accept_client(const int socket_fd, const int is_non_blocking, ErrorInfo* err) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return -1;
    }

    const int client_fd = accept(socket_fd, NULL, NULL);
    if(client_fd == -1) {
        if(is_non_blocking == TRUE && (errno == EWOULDBLOCK || errno == EAGAIN)) {
            printf("No pending client connections\n");
        } else {
            close(socket_fd);
            LOG_ERR("Failed to accept a client connection");
            SET_ERR(err, errno, "Failed to accept a client connection", strerror(errno));
            return -1;
        }
    }

    return client_fd;
}

int get_port_from_env(const char* env_var, ErrorInfo* err) {
    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was null\n");
        return -1;
    }

    const char* port_str = getenv(env_var);
    if(port_str == NULL) {
        LOG_ERR("Could not get port env variable");
        SET_ERR(err, -1, "Could not get port env variable", "");
        return -1;
    }

    const long int port = strtol(port_str, NULL, 10);
    if (errno != 0) {
        LOG_ERR("Could not get port number from port string");
        SET_ERR(err, -1, "Could not get port number from port string", "");
        return -1;
    }

    return (int)port;
}
