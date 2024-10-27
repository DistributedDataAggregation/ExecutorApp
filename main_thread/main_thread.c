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

int run_main_thread() {

    int socketfd = create_tcp_socket("0.0.0.0", TRUE, TRUE);

    int clientfd;
    while((clientfd = accept(socketfd, NULL, NULL)) == -1) {
        if(errno != EWOULDBLOCK) {
            close(socketfd);
            ERR("accept");
        }

        printf("no new client\n");
        sleep(1);
    }

    printf("Client connected\n");

    char buffer[1024];
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytes;
    if ((bytes = read(clientfd, buffer, sizeof(buffer))) == -1) {
        close(clientfd);
        close(socketfd);
        ERR("read");
    }

    printf("Read %ld bytes\n", bytes);
    printf("%s", buffer);


    char* lines = "3\n";
    ssize_t lines_len = strlen(lines);
    if((bytes = write(clientfd, lines, lines_len)) == -1) {
        close(clientfd);
        close(socketfd);
        ERR("write");
    }

    printf("Written %ld bytes\n", bytes);

    char* line = "War_gr1, war_gr2, rez1, rez2, rez3\n";
    for(int i = 0 ;i < 3 ;i++) {
        if((bytes = write(clientfd, line, strlen(line))) == -1) {
            close(clientfd);
            close(socketfd);
            ERR("write");
        }
        printf("Written %ld bytes\n", bytes);
    }

    if ((bytes = read(clientfd, buffer, sizeof(buffer))) == -1) {
        close(clientfd);
        close(socketfd);
        ERR("read");
    }

    close(clientfd);
    close(socketfd);
    return EXIT_SUCCESS;
}
