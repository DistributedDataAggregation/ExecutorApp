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

typedef struct {
    char ip_address[INET_ADDRSTRLEN];
    int socket;
} MainExecutorSocket;

typedef struct {
    MainExecutorSocket* sockets;
    size_t count;
    size_t capacity;
} MainExecutorsSockets;

void init_main_executors_sockets(MainExecutorsSockets* sockets, size_t capacity) {
    sockets->sockets = malloc(sizeof(MainExecutorSocket) * capacity);
    sockets->count = 0;
    sockets->capacity = capacity;
}

void free_main_executors_sockets(MainExecutorsSockets* sockets) {
    for (size_t i = 0; i < sockets->count; i++) {
        close(sockets->sockets[i].socket);
    }
    free(sockets->sockets);
}

int find_or_add_main_socket(MainExecutorsSockets* sockets, const char* ip_address, int port) {
    for (size_t i = 0; i < sockets->count; i++) {
        if (strcmp(sockets->sockets[i].ip_address, ip_address) == 0) {
            printf("Found main socket %s\n", sockets->sockets[i].ip_address);
            return sockets->sockets[i].socket;
        }
    }

    // Jeśli socket nie istnieje, tworzymy nowy
    if (sockets->count >= sockets->capacity) {
        sockets->capacity *= 2;
        sockets->sockets = realloc(sockets->sockets, sizeof(MainExecutorSocket) * sockets->capacity);
    }

    int new_socket = create_tcp_socket("0.0.0.0", TRUE, FALSE, 0);
    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, ip_address, &server_addr.sin_addr) <= 0) {
        perror("Invalid address/Address not supported");
        close(new_socket);
        return -1;
    }

    if (connect(new_socket, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("Connection failed");
        close(new_socket);
        return -1;
    }

    strncpy(sockets->sockets[sockets->count].ip_address, ip_address, INET_ADDRSTRLEN);
    sockets->sockets[sockets->count].socket = new_socket;
    sockets->count++;

    printf("Added new main socket to main_executors\n");
    return new_socket;
}

int handle_client(int client_fd, ClientArray* executors_client_array, int executors_socket_fd,
    MainExecutorsSockets* main_executors_sockets);

int run_main_thread() {

    printf("Running main thread\n");
    const char* controllers_port = "8080"; //getenv("EXECUTOR_CONTROLLER_PORT");
    if(controllers_port == NULL) {
        fprintf(stderr, "EXECUTOR_CONTROLLER_PORT not set\n");
        exit(EXIT_FAILURE);
    }

    const char* executors_port = "8081"; //getenv("EXECUTOR_EXECUTOR_PORT");
    if(executors_port == NULL) {
        fprintf(stderr, "EXECUTOR_EXECUTOR_PORT not set\n");
        exit(EXIT_FAILURE);
    }

    const int controllers_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
        TRUE, TRUE, atoi(controllers_port));
    const int executors_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
        TRUE, TRUE, atoi(executors_port));

    ClientArray controllers_client_array;
    init_client_array(&controllers_client_array, 10);

    ClientArray executors_client_array;
    init_client_array(&executors_client_array, 10);

    MainExecutorsSockets main_executors_sockets;
    init_main_executors_sockets(&main_executors_sockets, 10);

    while (1) {

        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(controllers_socket_fd, &read_fds);
        int max_fd = controllers_socket_fd;

        set_clients(&controllers_client_array, &max_fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

        if (select_result < 0) {
            perror("select");
            break;
        }

        accept_clients(&controllers_client_array, controllers_socket_fd, &read_fds);

        for (size_t i = 0; i < controllers_client_array.count; i++) {
            if (FD_ISSET(controllers_client_array.clients[i], &read_fds)) {
                if (handle_client(controllers_client_array.clients[i], &executors_client_array,
                    executors_socket_fd, &main_executors_sockets) != EXIT_SUCCESS) {
                    printf("Removing client: %d\n", controllers_client_array.clients[i]);
                    remove_client(&controllers_client_array, i);
                    i--;
                }
                fflush(stdout);
            }
        }
    }

    free_client_array(&controllers_client_array);
    free_client_array(&executors_client_array);
    close(controllers_socket_fd);
    return EXIT_SUCCESS;
}

int handle_client(int client_fd, ClientArray* executors_client_array, const int executors_socket_fd,
    MainExecutorsSockets* main_executors_sockets) {

    QueryRequest* request = parse_incoming_request(client_fd);
    HashTable* ht = run_request_on_worker_group(request);

    if (request->executor->is_current_node_main) {
        printf("This node is main\n");

        int collected = 0;
        const int others_count = request->executor->executors_count-1;

        while (collected < others_count) {

            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(executors_socket_fd, &read_fds);
            int max_fd = executors_socket_fd;

            set_clients(executors_client_array, &max_fd, &read_fds);

            struct timeval timeout;
            timeout.tv_sec = 1;
            timeout.tv_usec = 0;

            const int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

            if (select_result < 0) {
                perror("select");
                break;
            }

            accept_clients(executors_client_array, executors_socket_fd, &read_fds);

            for (size_t i = 0; i < executors_client_array->count && collected < others_count; i++) {
                if (FD_ISSET(executors_client_array->clients[i], &read_fds)) {
                    QueryResponse* response = parse_query_response(executors_client_array->clients[i]);
                    combine_table_with_response(ht, response);
                    collected++;
                }
            }
        }

        printf("Collected from other nodes\n");
        send_reponse(client_fd, ht);

    }

    else {
        printf("This node is slave\n");
        const int main_executor_socket = find_or_add_main_socket(main_executors_sockets,
            request->executor->main_ip_address, request->executor->main_port);
        send_reponse(main_executor_socket, ht);
        printf("Sent results to main\n");
    }

    free_hash_table(ht);
    query_request__free_unpacked(request, NULL);
    return EXIT_SUCCESS;
}

/*int handle_client(int clientfd) {
    QueryRequest* request = parse_incoming_request(clientfd);

    if(request->executor->is_current_node_main) {
        printf("This node is main\n");
    } else {
        printf("This node is slave\n");
    }

    const char* port_string = "8081"; //getenv("EXECUTOR_EXECUTOR_PORT");
    if(port_string == NULL) {
        fprintf(stderr, "EXECUTOR_EXECUTOR_PORT not set\n");
        exit(EXIT_FAILURE);
    }

    printf("Executor socket: %s\n", port_string);
    int executors_socket = -1;

    if (request->executor->is_current_node_main) {
        executors_socket = create_and_listen_on_tcp_socket("0.0.0.0", TRUE, FALSE, atoi(port_string));
    } else {
        executors_socket = create_tcp_socket("0.0.0.0", TRUE, FALSE, atoi(port_string));
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

    HashTable* ht = run_request_on_worker_group(request);

    if (request->executor->is_current_node_main) {
        int collected = 0;

        while(collected != others_count) {
            QueryResponse* response = parse_query_response(other_nodes_sockets[collected]);
            combine_table_with_response(ht, response);
            collected++;
        }
        printf("Collected from other nodes\n");
        send_reponse(clientfd, ht);
    } else {
        send_reponse(executors_socket, ht);
        printf("Sent results to main\n");
    }

    free_hash_table(ht);

    query_request__free_unpacked(request, NULL);
    close(executors_socket);
    for(int i = 0; i < others_count; i++) {
        close(other_nodes_sockets[i]);
    }
    free(other_nodes_sockets);
    return EXIT_SUCCESS;
}*/


