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

#include "client_array.h"
#include "executors_server.h"

int main_thread_handle_client(int client_fd, ClientArray* executors_client_array, int executors_socket_fd,
    MainExecutorsSockets* main_executors_sockets);

int main_thread_run() {

    printf("Running main thread\n");
    const int controllers_port = get_port_from_env("EXECUTOR_CONTROLLER_PORT");
    const int executors_port = get_port_from_env("EXECUTOR_EXECUTOR_PORT");

    const int controllers_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
        TRUE, TRUE, controllers_port);
    const int executors_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
        TRUE, TRUE, executors_port);

    ClientArray controllers_client_array;
    client_array_init(&controllers_client_array, 10);

    ClientArray executors_client_array;
    client_array_init(&executors_client_array, 10);

    MainExecutorsSockets main_executors_sockets;
    executors_server_init_main_executors_sockets(&main_executors_sockets, 10);

    while (1) {

        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(controllers_socket_fd, &read_fds);
        int max_fd = controllers_socket_fd;

        client_array_set_clients(&controllers_client_array, &max_fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

        if (select_result < 0) {
            perror("select");
            break;
        }

        client_array_accept_clients(&controllers_client_array, controllers_socket_fd, &read_fds);

        for (size_t i = 0; i < controllers_client_array.count; i++) {
            if (FD_ISSET(controllers_client_array.clients[i], &read_fds)) {
                if (main_thread_handle_client(controllers_client_array.clients[i], &executors_client_array,
                    executors_socket_fd, &main_executors_sockets) != EXIT_SUCCESS) {
                    printf("Removing client: %d\n", controllers_client_array.clients[i]);
                    client_array_remove_client(&controllers_client_array, i);
                    i--;
                }
                fflush(stdout);
            }
        }
    }

    client_array_free(&controllers_client_array);
    client_array_free(&executors_client_array);
    close(controllers_socket_fd);
    return EXIT_SUCCESS;
}

int main_thread_handle_client(int client_fd, ClientArray* executors_client_array, const int executors_socket_fd,
    MainExecutorsSockets* main_executors_sockets) {

    QueryRequest* request = parse_incoming_request(client_fd);
    HashTable* ht = NULL;
    int worker_group_result = worker_group_run_request(request, &ht);

    if(worker_group_result == -1)
    {
        perror("Failed to run worker group");
        exit(EXIT_FAILURE);
    }

    if (request->executor->is_current_node_main) {
        printf("This node is main\n");

        int collected = 0;
        const int others_count = request->executor->executors_count-1;

        while (collected < others_count) {

            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(executors_socket_fd, &read_fds);
            int max_fd = executors_socket_fd;

            client_array_set_clients(executors_client_array, &max_fd, &read_fds);

            struct timeval timeout;
            timeout.tv_sec = 1;
            timeout.tv_usec = 0;

            const int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

            if (select_result < 0) {
                perror("select");
                break;
            }

            client_array_accept_clients(executors_client_array, executors_socket_fd, &read_fds);

            for (size_t i = 0; i < executors_client_array->count && collected < others_count; i++) {
                if (FD_ISSET(executors_client_array->clients[i], &read_fds)) {
                    QueryResponse* response = parse_query_response(executors_client_array->clients[i]);
                    hash_table_combine_table_with_response(ht, response);
                    collected++;
                }
            }
        }

        printf("Collected from other nodes\n");
        send_reponse(client_fd, ht);

    }

    else {
        printf("This node is slave\n");
        const int main_executor_socket = executors_server_find_or_add_main_socket(main_executors_sockets,
            request->executor->main_ip_address, request->executor->main_port);
        send_reponse(main_executor_socket, ht);
        printf("Sent results to main\n");
    }

    hash_table_free(ht);
    query_request__free_unpacked(request, NULL);
    return EXIT_SUCCESS;
}