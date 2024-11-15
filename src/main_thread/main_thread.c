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

    int socketfd = create_tcp_socket("0.0.0.0", TRUE, TRUE);
    int clientfd;
    while ((clientfd = accept_client(socketfd, TRUE)) == -1) {
        sleep(1);
    }
    printf("Client connected\n");

    QueryRequest* request = parse_incoming_request(clientfd);

    HashTable* ht = run_request_on_worker_group(request);

    QueryResponse* response = convert_table(ht);

    ssize_t size = query_response__get_packed_size(response);

    uint8_t* buffer = (uint8_t*)malloc(sizeof(uint8_t)*size);
    if (buffer == NULL) {
        ERR_AND_EXIT("malloc");
    }
    memset(buffer, 0, size);

    int size_to_send = htonl(size);
    if(write(clientfd, &size_to_send, sizeof(size_to_send)) <= 0) {
        ERR_AND_EXIT("send");
    }

    int stored = query_response__pack(response, buffer);
    if(stored != size) {
        ERR_AND_EXIT("results__pack");
    }

    if(send(clientfd, buffer, size, 0) != size) {
        ERR_AND_EXIT("send");
    }

    // for(int i=0;i<response.n_values;i++) {
    //     free(response.values[i]->grouping_value);
    //     int results_count = response.values[i]->n_results;
    //     for(int j=0; j< results_count; j++) {
    //         if(response.values[i]->results[j]->result_types_case == RESULT__RESULT_TYPES_COUNTED_RESULT)
    //             free(response.values[i]->results[j]);
    //     }
    //     free(response.values[i]->results);
    // }
    // free(response.values);


    // print(ht);
    // int total_count =0;
    // for(int i=0;i<ht->size;i++) {
    //     HashTableEntry* entry = ht->table[i];
    //     while(entry != NULL) {
    //         total_count+= entry->values[0].count;
    //         entry = entry->next;
    //     }
    // }

    // printf("Total count: %d\n", total_count);

    free_hash_table(ht);
    // TODO: replace this with an actual creation of a response entity and sending it back to controller
    // Results results = RESULTS__INIT;
    // results.n_values = 1;
    // results.values = malloc(sizeof(Value*)*results.n_values);
    // if(results.values == NULL) {
    //     ERR_AND_EXIT("malloc");
    // }
    //
    // Value value = VALUE__INIT;
    // value.grouping_value = "war1|war2|war3|war4|war5";
    // value.n_result = 2;
    // value.result = malloc(sizeof(Result*)*value.n_result);
    // value.n_operation = 2;
    // value.operation = (malloc(sizeof(Aggregate*)*value.n_operation));
    //
    // Aggregate aggregate1 = AGGREGATE__Minimum;
    // Aggregate aggregate2 = AGGREGATE__Average;
    //
    // value.operation[0] = aggregate1;
    // value.operation[1] = aggregate2;
    //
    // Result result1 = RESULT__INIT;
    // result1.result_types_case = RESULT__RESULT_TYPES_SINGLE_RESULT;
    // result1.singleresult = 1001;
    //
    // Result result2 = RESULT__INIT;
    // result2.result_types_case = RESULT__RESULT_TYPES_COUNTED_RESULT;
    // CountedResult counted_result = COUNTED_RESULT__INIT;
    // counted_result.count = 1234;
    // counted_result.value = 123141241;
    // result2.countedresult = &counted_result;
    //
    // value.result[0] = &result1;
    // value.result[1] = &result2;
    //
    // results.values[0] = &value;
    //
    // ssize_t size = results__get_packed_size(&results);
    //
    // uint8_t* buffer = (uint8_t*)malloc(sizeof(uint8_t)*size);
    // if (buffer == NULL) {
    //     ERR_AND_EXIT("malloc");
    // }
    // memset(buffer, 0, size);
    //
    // int size_to_send = htonl(size);
    // if(write(clientfd, &size_to_send, sizeof(size_to_send)) <= 0) {
    //     ERR_AND_EXIT("send");
    // }
    //
    // int stored = results__pack(&results, buffer);
    // if(stored != size) {
    //     ERR_AND_EXIT("results__pack");
    // }
    //
    // if(send(clientfd, buffer, size, 0) != size) {
    //     ERR_AND_EXIT("send");
    // }
    //
    // free(value.result);
    // free(value.operation);
    // free(results.values);
    // free(buffer);
    query_request__free_unpacked(request, NULL);
    close(clientfd);
    close(socketfd);
    return EXIT_SUCCESS;
}


