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

int handle_client(int clientfd);

int run_main_thread() {

    const char* port_string = getenv("EXECUTOR_CONTROLLER_PORT");
    if(port_string == NULL) {
        fprintf(stderr, "EXECUTOR_CONTROLLER_PORT not set\n");
        exit(EXIT_FAILURE);
    }
    printf("Controller socket: %s\n", port_string);

    int socketfd = create_tcp_socket("0.0.0.0", TRUE, TRUE, atoi(port_string));

    ClientArray client_array;
    init_client_array(&client_array, 10);

    while (1) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(socketfd, &read_fds);

        int max_fd = socketfd;
        set_clients(&client_array, &max_fd, &read_fds);

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;

        int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

        if (select_result < 0) {
            perror("select");
            break;
        }

        accept_clients(&client_array, socketfd, &read_fds);

        for (size_t i = 0; i < client_array.count; i++) {
            if (FD_ISSET(client_array.clients[i], &read_fds)) {
                if (handle_client(client_array.clients[i]) == -1) {
                    printf("Removing client: %d\n", client_array.clients[i]);
                    remove_client(&client_array, i);
                    i--;
                }
            }
        }
    }

    free_client_array(&client_array);
    close(socketfd);
    return EXIT_SUCCESS;
}

int handle_client(int clientfd) {
    QueryRequest* request = parse_incoming_request(clientfd);
    if (!request) {
        fprintf(stderr, "Failed to parse request\n");
        return -1;
    }

    if (request->executor->is_current_node_main) {
        printf("This node is main\n");
    } else {
        printf("This node is slave\n");
    }

    HashTable* ht = run_request_on_worker_group(request);

    send_reponse(clientfd, ht);

    free_hash_table(ht);
    query_request__free_unpacked(request, NULL);

    return 0;
}
