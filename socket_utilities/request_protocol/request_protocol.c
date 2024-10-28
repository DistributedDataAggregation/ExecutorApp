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
int get_elements_count(char* buffer, char** space_pos);

int parse_incoming_request(int client_socket, Request* request) {
    char buffer[BUFFER_SIZE] = {0};
    ssize_t bytes = 0;
    ssize_t total_bytes_read = 0;

    if ((bytes = read(client_socket, buffer, BUFFER_SIZE)) < 0) {
        ERR_AND_EXIT("read");

    }

    char* space_pos;
    int files_count = get_elements_count(buffer, &space_pos);

    printf("Files count: %d\n", files_count);
    request->files_count = files_count;
    request->files = malloc(files_count * sizeof(char*));


    char* remaining_string = space_pos + 1;

    int success = parse_delimited_strings(files_count, request->files, ',', &remaining_string);
    if(success != EXIT_SUCCESS) {
        fprintf(stderr, "failed to parse file name strings");
        return EXIT_FAILURE;
    }

    remaining_string += 1;
    space_pos = strchr(remaining_string, ' ');
    int grouping_columns_count = get_elements_count(remaining_string, &space_pos);
    remaining_string = space_pos + 1;

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
    int aggregations_count = get_elements_count(remaining_string, &space_pos);
    remaining_string = space_pos + 1;

    char** aggregations = malloc(aggregations_count * sizeof(char*));
    request->aggregation_columns_count = aggregations_count;
    request->aggregations = malloc(aggregations_count * sizeof(Aggregation));
    success = parse_delimited_strings(aggregations_count, aggregations, ',', &remaining_string);

    if(success != EXIT_SUCCESS) {
        fprintf(stderr, "failed to parse aggregation strings\n");
        return EXIT_FAILURE;
    }

    for(int i=0; i< aggregations_count; i++) {
        request->aggregations[i] = parse_aggregation(aggregations[i]);
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


int get_elements_count(char* buffer, char** space_pos) {

    (*space_pos) = strchr(buffer, ' ');
    if((*space_pos) != NULL) {
        (**space_pos) = '\0';
    } else {
        printf("Invalid request string\n");
    }

    int elements_count = atoi(buffer);
    if(elements_count < 1) {
        fprintf(stderr, "Invalid number of elements count: %d\n", elements_count);
        return EXIT_FAILURE;
    }

    return elements_count;
}