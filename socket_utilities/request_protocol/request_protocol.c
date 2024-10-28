//
// Created by karol on 27.10.24.
//

#include "request_protocol.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "error_utilites.h"

#define BUFFER_SIZE 1024

int parse_delimited_strings(int strings_count, char** strings, char delimiter, char** remaining_string);
char* move_pointer_by_n_characters(char* string, int n);
Aggregation parse_aggregation(char* aggregation_string);
Operation parse_operation(char* operation_string);

int parse_incoming_request(int client_socket, Request* request) {
    char buffer[BUFFER_SIZE];
    memset(buffer, 0, sizeof(buffer));
    ssize_t bytes = 0;
    ssize_t total_bytes_read = 0;

    char* space_pos = 0;
    while((bytes = read(client_socket, buffer + total_bytes_read, sizeof(buffer) - total_bytes_read - 1)) > 0) {
        total_bytes_read += bytes;

        space_pos = strchr(buffer, ' ');
        if(space_pos != NULL) {
            *space_pos = '\0';
            break;
        }

        if(total_bytes_read >= BUFFER_SIZE - 1) {
            fprintf(stderr, "Request buffer overflow");
            return EXIT_FAILURE;
        }
    }

    if(bytes < 0) {
        ERR_AND_EXIT("read");
    }

    printf("Read %ld bytes\n", bytes);

    int files_count = atoi(buffer);
    if(files_count < 1) {
        fprintf(stderr, "Invalid number of files count: %d\n", files_count);
        return EXIT_FAILURE;
    }

    printf("Files count: %d\n", files_count);
    request->files_count = files_count;
    request->files = malloc(files_count * sizeof(char*));


    char* remaining_string = space_pos + 1;

    int success = parse_delimited_strings(files_count, request->files, ',', &remaining_string);
    if(success != EXIT_SUCCESS) {
        fprintf(stderr, "failed to parse file name strings");
        return EXIT_FAILURE;
    }

    for(int i=0; i< files_count; i++) {
        printf("File number %d: %s\n", i, request->files[i]);
    }

    remaining_string += 1;
    printf("Remianing string:%s\n", remaining_string);

    space_pos = strchr(remaining_string, ' ');
    int grouping_columns_count = 0;
    if(space_pos != NULL) {
        *space_pos = '\0';

        grouping_columns_count = atoi(remaining_string);
        remaining_string = space_pos + 1;
        if(grouping_columns_count < 1) {
            fprintf(stderr, "Invalid grouping columns count\n");
            return EXIT_FAILURE;
        }

    }else {
        fprintf(stderr, "Incorrect format for providing grouping columns\n");
        return EXIT_FAILURE;
    }

    printf("Grouping columns count: %d\n", grouping_columns_count);
    request->grouping_columns_count = grouping_columns_count;

    success = parse_delimited_strings(grouping_columns_count, request->grouping_columns, ',', &remaining_string);
    if(success != EXIT_SUCCESS) {
        fprintf(stderr, "failed to parse grouping strings\n");
        return EXIT_FAILURE;
    }

    for(int i=0; i< grouping_columns_count; i++) {
        printf("Grouping column number %d: %s\n", i, request->grouping_columns[i]);
    }

    remaining_string+=1;
    space_pos = strchr(remaining_string, ' ');
    int aggregations_count = 0;
    if(space_pos != NULL) {
        *space_pos = '\0';

        aggregations_count = atoi(remaining_string);
        remaining_string = space_pos + 1;
        if(aggregations_count < 1) {
            fprintf(stderr, "Invalid aggregations count\n");
            return EXIT_FAILURE;
        }

    }else {
        fprintf(stderr, "Incorrect format for providing aggregation operations\n");
        return EXIT_FAILURE;
    }

    char** aggregations = malloc(aggregations_count * sizeof(char*));
    request->aggregation_columns_count = aggregations_count;
    request->aggregations = malloc(aggregations_count * sizeof(Aggregation));
    success = parse_delimited_strings(aggregations_count, aggregations, ',', &remaining_string);

    for(int i=0; i< aggregations_count; i++) {
        request->aggregations[i] = parse_aggregation(aggregations[i]);
    }

    for(int i=0; i< aggregations_count; i++) {
        printf("Aggregation column number %d: %s\n", i, request->aggregations[i].column);
    }

    free(aggregations);

    return EXIT_SUCCESS;
}


int parse_delimited_strings(int strings_count, char** strings, char delimiter, char** remaining_string) {
    for(int i = 0; i < strings_count; i++) {
        char* delimiter_pos = strchr(*remaining_string, delimiter);
        if(delimiter_pos != NULL) {
            *delimiter_pos = '\0';
            strings[i] = strdup(*remaining_string);
            (*remaining_string) = delimiter_pos + 1;
        }else {
            printf("Error couldn't find delimiter!");
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}

Aggregation parse_aggregation(char* aggregation_string) {
    Aggregation aggregation;
    char* space_pos = strchr(aggregation_string, ' ');
    if(space_pos == NULL) {
        fprintf(stderr, "Wrong format of aggregation string expected [column_name aggregation_function] got [%s]\n", aggregation_string);
        return aggregation;
    }
    *space_pos = '\0';
    aggregation.column = strdup(aggregation_string);

    char* remaining_string = space_pos + 1;
    aggregation.operation = parse_operation(remaining_string);

    return aggregation;
}

Operation parse_operation(char* operation_string) {
    if(strcmp(operation_string, summation) == 0) {
        return SUM;
    }
    if(strcmp(operation_string, minimum) == 0) {
        return MIN;
    }
    if(strcmp(operation_string, maximum) == 0) {
        return MAX;
    }
    if(strcmp(operation_string, average) == 0) {
        return AVERAGE;
    }

    return UNDEFINED;
}
