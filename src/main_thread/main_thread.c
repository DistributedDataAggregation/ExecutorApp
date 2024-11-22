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


int run_main_thread() {

    const char* port_string = getenv("EXECUTOR_CONTROLLER_PORT");
    if(port_string == NULL) {
        fprintf(stderr, "EXECUTOR_CONTROLLER_PORT not set\n");
        exit(EXIT_FAILURE);
    }
    printf("Controller socket: %s\n", port_string);


    int socketfd = create_tcp_socket("0.0.0.0", TRUE, TRUE, atoi(port_string));

    int clientfd;
    while ((clientfd = accept_client(socketfd, TRUE)) == -1) {
        sleep(1);
    }

    printf("Client connected\n");

    QueryRequest* request = parse_incoming_request(clientfd);

    if(request->executor->is_current_node_main) {
        printf("This node is main\n");
    } else {
        printf("This node is slave\n");
    }

    HashTable* ht = run_request_on_worker_group(request);

    send_reponse(clientfd, ht);
    free_hash_table(ht);

    query_request__free_unpacked(request, NULL);
    close(clientfd);
    close(socketfd);
    return EXIT_SUCCESS;
}


