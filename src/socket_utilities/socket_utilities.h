//
// Created by karol on 27.10.24.
//

#ifndef SOCKET_UTILITIES_H
#define SOCKET_UTILITIES_H

#include <error_handling.h>

int create_tcp_socket(const char* address_string, int so_reuse, int non_blocking, int port, ErrorInfo* err);
int create_and_listen_on_tcp_socket(const char* address_string, int so_reuse, int non_blocking, int port, ErrorInfo* err);
int accept_client(int socket_fd, int is_non_blocking, ErrorInfo* err);
int get_port_from_env(const char* env_var, ErrorInfo* err);

#endif //SOCKET_UTILITIES_H