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
#include "value.h"
#include "query_request/query_request.pb-c.h"

int run_main_thread() {

    int socketfd = create_tcp_socket("0.0.0.0", TRUE, TRUE);
    int clientfd;
    while ((clientfd = accept_client(socketfd, TRUE)) == -1) {
        sleep(1);
    }

    printf("Client connected\n");

    char buffer[1024];
    ssize_t bytes;

    QueryRequest* request = parse_incoming_request(clientfd);
    query_request__free_unpacked(request, NULL);

    Value value;
    value.results.count = 3;
    value.results.operations = (Operation*)malloc(sizeof(Operation) * value.results.count);
    if(value.results.operations == NULL) {
        ERR_AND_EXIT("malloc");
    }

    value.results.values = (ResultType*)malloc(sizeof(ResultType) * value.results.count);
    if(value.results.values == NULL) {
        ERR_AND_EXIT("malloc");
    }

    value.grouping_value = "War_gr1|war_gr2";
    for(int i = 0; i < value.results.count; i++) {
        value.results.operations[i] = SUM;
        value.results.values[i].singleResult.value = 1000001;
    }

    char* value_string = to_string(value);
    free(value_string);

    free(value.results.operations);
    free(value.results.values);

    char* lines = "3\n";
    ssize_t lines_len = strlen(lines);
    if((bytes = write(clientfd, lines, lines_len)) == -1) {
        close(clientfd);
        close(socketfd);
        ERR_AND_EXIT("write");
    }

    printf("Written %ld bytes\n", bytes);

    char* line = "War_gr1, war_gr2, rez1, rez2, rez3\n";
    for(int i = 0 ;i < 3 ;i++) {
        if((bytes = write(clientfd, line, strlen(line))) == -1) {
            close(clientfd);
            close(socketfd);
            ERR_AND_EXIT("write");
        }
        printf("Written %ld bytes\n", bytes);
    }

    if ((bytes = read(clientfd, buffer, sizeof(buffer))) == -1) {
        close(clientfd);
        close(socketfd);
        ERR_AND_EXIT("read");
    }

    close(clientfd);
    close(socketfd);
    return EXIT_SUCCESS;
}
