//
// Created by karol on 26.10.24.
//

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#include "main_thread.h"

#include <errno.h>
#include <string.h>

#include "error_utilites.h"
#include "socket_utilities.h"
#include "boolean.h"
#include "request_protocol/request_protocol.h"
#include "query_request.pb-c.h"
#include "query_response.pb-c.h"
#include <google/protobuf-c/protobuf-c.h>

#include <parquet-glib/arrow-file-reader.h>

#include "hash_table.h"
#include "worker_group.h"
#include "hash_table_to_query_response_converter.h"

#include "controllers_server.h"

int main_thread_handle_client(int clientfd);

int main_thread_run() {

    const char* exec_ctrl_port_str = getenv("EXECUTOR_CONTROLLER_PORT");
    if(exec_ctrl_port_str == NULL) {
        fprintf(stderr, "EXECUTOR_CONTROLLER_PORT not set\n");
        exit(EXIT_FAILURE);
    }
    printf("Controller socket: %s\n", exec_ctrl_port_str);

    int socketfd = create_and_listen_on_tcp_socket("0.0.0.0", TRUE, TRUE, atoi(exec_ctrl_port_str));

    ClientArray client_array;
    controller_server_init_client_array(&client_array, 10);

    while (1) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(socketfd, &read_fds);

        int max_fd = socketfd;
        controller_server_set_clients(&client_array, &max_fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

        if (select_result < 0) {
            perror("select");
            break;
        }

        controller_server_accept_clients(&client_array, socketfd, &read_fds);

        for (size_t i = 0; i < client_array.count; i++) {
            if (FD_ISSET(client_array.clients[i], &read_fds)) {
                if (main_thread_handle_client(client_array.clients[i]) == -1) {
                    printf("Removing client: %d\n", client_array.clients[i]);
                    controller_server_remove_client(&client_array, i);
                    i--;
                }
            }
        }
    }

    close(socketfd);
    return EXIT_SUCCESS;
}

int main_thread_handle_client(int clientfd) {
    QueryRequest* request = parse_incoming_request(clientfd);

    if(request->executor->is_current_node_main) {
        printf("This node is main\n");
    } else {
        printf("This node is slave\n");
    }

    const char* exec_exec_port_string = getenv("EXECUTOR_EXECUTOR_PORT");
    if(exec_exec_port_string == NULL) {
        fprintf(stderr, "EXECUTOR_EXECUTOR_PORT not set\n");
        exit(EXIT_FAILURE);
    }
    printf("Executor socket: %s\n", exec_exec_port_string);
    int executors_socket = -1;

    if (request->executor->is_current_node_main) {
        executors_socket = create_and_listen_on_tcp_socket("0.0.0.0", TRUE, FALSE, atoi(exec_exec_port_string));
    } else {
        executors_socket = create_tcp_socket("0.0.0.0", TRUE, FALSE, atoi(exec_exec_port_string));
    }

    int others_count = request->executor->executors_count-1;
    int* other_nodes_sockets = malloc(sizeof(int) * others_count);
    if(request->executor->is_current_node_main) {
        for(int i = 0; i < others_count; i++) {
            other_nodes_sockets[i] = accept_client(executors_socket, FALSE);
        }

        printf("Accepted clients\n");
    } else {
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(request->executor->main_port);

        // 3. Convert the IP address from text to binary form
        if (inet_pton(AF_INET, request->executor->main_ip_address, &server_addr.sin_addr) <= 0) {
            perror("Invalid address/ Address not supported");
            close(executors_socket);
            exit(EXIT_FAILURE);
        }

        // 4. Connect to the server
        int attempts = 9;
        int failure = 1;
        while(failure && attempts-->0) {

            if (connect(executors_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
                failure = 1;
                perror("Failed to connect to server");
                sleep(1);
            } else {
                failure = 0;
            }
            attempts--;
        }

        if (failure && connect(executors_socket, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
            perror("Connection failed");
            close(executors_socket);
            exit(EXIT_FAILURE);
        }

        printf("Connected to %s on port %d\n", request->executor->main_ip_address, request->executor->main_port);
    }

    HashTable* ht = NULL;
    int worker_group_result = worker_group_run_request(request, &ht);

    if(worker_group_result == -1)
    {
        perror("Failed to run worker group");
        close(executors_socket);
        exit(EXIT_FAILURE);
    }

    if (request->executor->is_current_node_main) {
        int collected = 0;

        while(collected != others_count) {
            QueryResponse* response = parse_query_response(other_nodes_sockets[collected]);
            hash_table_combine_table_with_response(ht, response);
            collected++;
        }
        printf("Collected from other nodes\n");
        send_reponse(clientfd, ht);
    } else {
        send_reponse(executors_socket, ht);
        printf("Sent results to main\n");
    }

    hash_table_free(ht);

    query_request__free_unpacked(request, NULL);
    close(executors_socket);
    for(int i = 0; i < others_count; i++) {
        close(other_nodes_sockets[i]);
    }
    free(other_nodes_sockets);
    return 1;
}


