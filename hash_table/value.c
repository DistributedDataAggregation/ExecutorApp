//
// Created by karol on 29.10.24.
//

#include "value.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "boolean.h"

#define SEPARATOR ','
#define SEPARATOR_SIZE 1

int is_single_result_operation(const Operation operation);
long get_length(long value);

char* to_string(const Value value) {
    int string_length = 0;
    string_length += strlen(value.grouping_value);

    int* results_lengths = (int*)(malloc(sizeof(int)* value.results.count));

    for(int i = 0; i < value.results.count; i++) {
        if(is_single_result_operation(value.results.operations[i])) {
            results_lengths[i] = get_length(value.results.values[i].singleResult.value);
            string_length+= results_lengths[i];

        } else if(value.results.operations[i] == AVERAGE) {
            results_lengths[i] = get_length(value.results.values[i].countedResult.value/value.results.values[i].countedResult.count);
            string_length+=results_lengths[i] ;
        }
        string_length+=SEPARATOR_SIZE;
    }

    char* result = (char*)malloc((string_length + 1) * sizeof(char));
    strcpy(result, value.grouping_value);
    char* separator_string = malloc(SEPARATOR_SIZE * sizeof(char));
    separator_string[0] = SEPARATOR;
    for(int i = 0; i < value.results.count; i++) {
        strcat(result, separator_string);
        const int current_length = results_lengths[i];
        char* value_string = malloc(sizeof(char) * current_length);

        if(is_single_result_operation(value.results.operations[i])) {
            snprintf(value_string, current_length, "%ld", value.results.values[i].singleResult.value);
        } else {
            snprintf(value_string, current_length, "%ld", value.results.values[i].singleResult.value/ value.results.values[i].countedResult.count);
        }

        strcat(result, value_string);
        free(value_string);
    }

    free(results_lengths);
    free(separator_string);

    printf("Result printed: %s\n", result);
    return result;
}

int is_single_result_operation(const Operation operation) {
    switch(operation) {
        case MIN:
        case MAX:
        case SUM:
            return TRUE;
        case AVERAGE:
            return FALSE;
        default:
            return FALSE;
    }
}


long get_length(long value) {

    int result = 0;
    do {
        ++result;
        value = value /10;
    }while(value);

    return result;
}