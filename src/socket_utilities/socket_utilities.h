//
// Created by karol on 27.10.24.
//

#ifndef SOCKET_UTILITIES_H
#define SOCKET_UTILITIES_H

int create_tcp_socket(char* address_string, int so_reuse, int non_blocking, int port);
int create_and_listen_on_tcp_socket(char* address_string, int so_reuse, int non_blocking, int port);
int accept_client(int socketfd, int is_non_blocking);

#endif //SOCKET_UTILITIES_H