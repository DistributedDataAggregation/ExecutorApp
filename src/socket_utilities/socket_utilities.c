//
// Created by karol on 27.10.24.
//
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>

#include "socket_utilities.h"
#include "error_utilites.h"
#include "boolean.h"

void set_socket_to_nonblocking(int socketfd);
void set_socket_to_reuse(int socketfd);

void set_socket_to_nonblocking(int socketfd) {
    int flags = fcntl(socketfd, F_GETFL, 0);
    if (flags == -1) {
        close(socketfd);
        ERR_AND_EXIT("fcntl");
    }

    if (fcntl(socketfd, F_SETFL, flags | O_NONBLOCK) == -1) {
        close(socketfd);
        ERR_AND_EXIT("fcntl");
    }
}

int create_and_listen_on_tcp_socket(char* address_string, int so_reuse, int non_blocking, int port) {
    int socketfd = create_tcp_socket(address_string, so_reuse, non_blocking, port);

    if(listen(socketfd, 3) == -1) {
        close(socketfd);
        ERR_AND_EXIT("listen");
    }

    return socketfd;
}

int create_tcp_socket(char* address_string, int so_reuse, int non_blocking, int port) {
    int socketfd;
    if ((socketfd = socket(AF_INET, SOCK_STREAM, 0)) < 0 ) {
        ERR_AND_EXIT("socket");
    }

    setsockopt(socketfd, SOL_SOCKET, TCP_NODELAY, (int[]){1}, sizeof(int));

    if (non_blocking == TRUE) {
        set_socket_to_nonblocking(socketfd);
    }

    if(so_reuse == TRUE) {
        set_socket_to_reuse(socketfd);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;

    address.sin_port = htons(port);

    if (inet_aton(address_string, &address.sin_addr) ==  0) {
        close(socketfd);
        ERR_AND_EXIT("inet_aton");
    }
    if(bind(socketfd, (struct sockaddr*)&address, sizeof(address)) == -1) {
        close(socketfd);
        ERR_AND_EXIT("bind");
    }
    return socketfd;
}

void set_socket_to_reuse(int socketfd) {
    int opt = 1;
    if (setsockopt(socketfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(socketfd);
        ERR_AND_EXIT("setsockopt");
    }
}

int accept_client(int socketfd, int is_non_blocking) {
    int clientfd = accept(socketfd, NULL, NULL);
    if(clientfd == -1) {
        if(errno != EWOULDBLOCK) {
            close(socketfd);
            ERR_AND_EXIT("accept");
        } else if(is_non_blocking == TRUE) {
            printf("no new client\n");
        } else {
            close(socketfd);
            ERR_AND_EXIT("accept");
        }
    }

    return clientfd;
}


