//
// Created by karol on 26.10.24.
//

#include <google/protobuf-c/protobuf-c.h>
#include <parquet-glib/arrow-file-reader.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include "boolean.h"
#include "client_array.h"
#include "error_handling.h"
#include "executors_server.h"
#include "hash_table.h"
#include "main_thread.h"

#include "hash_table_interface.h"
#include "query_request.pb-c.h"
#include "query_response.pb-c.h"
#include "request_protocol/request_protocol.h"
#include "socket_utilities.h"
#include "worker_group.h"
#include "logging.h"


#define INITIAL_SIZE 10

void main_thread_handle_client(int client_fd, ClientArray* executors_client_array, int executors_socket_fd,
                               MainExecutorsSockets* main_executors_sockets, ErrorInfo* err);

int main_thread_run()
{
    errno = 0;

    LOG("Starting main thread\n");
    ErrorInfo error_info = {0};

    const int controllers_port = get_port_from_env("EXECUTOR_CONTROLLER_PORT", &error_info);
    const int executors_port = get_port_from_env("EXECUTOR_EXECUTOR_PORT", &error_info);
    if (error_info.error_code != NO_ERROR)
        return EXIT_FAILURE;

    const int controllers_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
                                                                      TRUE, TRUE, controllers_port, &error_info);
    const int executors_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
                                                                    TRUE, TRUE, executors_port, &error_info);
    if (error_info.error_code != NO_ERROR)
    {
        close(controllers_socket_fd);
        close(executors_socket_fd);
        return EXIT_FAILURE;
    }

    LOG("Listening on ports %d and %d\n", controllers_port, executors_port);

    ClientArray controllers_client_array;
    client_array_init(&controllers_client_array, INITIAL_SIZE, &error_info);

    ClientArray executors_client_array;
    client_array_init(&executors_client_array, INITIAL_SIZE, &error_info);

    MainExecutorsSockets main_executors_sockets;
    executors_server_init_main_executors_sockets(&main_executors_sockets, INITIAL_SIZE, &error_info);

    if (error_info.error_code != NO_ERROR)
    {
        client_array_free(&controllers_client_array);
        client_array_free(&executors_client_array);
        executors_server_free(&main_executors_sockets);
        close(controllers_socket_fd);
        close(executors_socket_fd);
        return EXIT_FAILURE;
    }

    while (1)
    {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(controllers_socket_fd, &read_fds);
        int max_fd = controllers_socket_fd;

        client_array_set_clients(&controllers_client_array, &max_fd, &read_fds);

        struct timeval timeout = {
            .tv_sec = 1,
            .tv_usec = 0
        };

        const int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);
        if (select_result < 0)
        {
            LOG_ERR("Failed on select");
            break;
        }

        client_array_accept_clients(&controllers_client_array, controllers_socket_fd, &read_fds, &error_info);
        if (error_info.error_code != NO_ERROR)
        {
            CLEAR_ERR(&error_info); // TODO handle failed connection from controller
        }

        for (size_t i = 0; i < controllers_client_array.count; i++)
        {
            if (FD_ISSET(controllers_client_array.clients[i], &read_fds))
            {
                main_thread_handle_client(controllers_client_array.clients[i], &executors_client_array,
                                          executors_socket_fd, &main_executors_sockets, &error_info);
                if (error_info.error_code != NO_ERROR)
                {
                    // LOG("Removing client: %d\n", controllers_client_array.clients[i]);
                    // client_array_remove_client(&controllers_client_array, i);
                    // i--;
                    // TODO handle failed request from controller (connections errors and error while sending failure response)
                    CLEAR_ERR(&error_info);
                }
                fflush(stdout);
            }
        }
    }

    client_array_free(&controllers_client_array);
    client_array_free(&executors_client_array);
    executors_server_free(&main_executors_sockets);
    close(controllers_socket_fd);
    close(executors_socket_fd);
    return EXIT_SUCCESS;
}

void main_thread_handle_client(const int client_fd, ClientArray* executors_client_array, const int executors_socket_fd,
                               MainExecutorsSockets* main_executors_sockets, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return; // TODO handle ??
    }

    QueryRequest* request = parse_incoming_request(client_fd, err);
    if (err->error_code != NO_ERROR)
    {
        return; // TODO handle ?? error message should be send to controller but we dont know with executor is main
    }

    HashTable* ht = NULL;
    HashTableInterface* ht_interface = create_default_hash_table_interface();


    if (request->executor->is_current_node_main)
    {
        LOG("This node is main\n");
        worker_group_run_request(request, &ht, ht_interface, err);

        if (err->error_code != NO_ERROR)
        {
            // TODO send_failure response right away or wait for other executors to send their response?
            // if we wait, we need to initialize the hash table because it may be null
        }

        int collected = 0;
        const int others_count = request->executor->executors_count - 1;

        while (collected < others_count)
        {
            // TODO timeout??

            fd_set read_fds;
            FD_ZERO(&read_fds);
            FD_SET(executors_socket_fd, &read_fds);
            int max_fd = executors_socket_fd;

            client_array_set_clients(executors_client_array, &max_fd, &read_fds);

            struct timeval timeout = {
                .tv_sec = 1,
                .tv_usec = 0
            };

            const int select_result = select(max_fd + 1, &read_fds, NULL, NULL, &timeout);

            if (select_result < 0)
            {
                LOG_ERR("Failed on select waiting for other executors");
                SET_ERR(err, errno, "Failed on select waiting for other executors", strerror(errno));
                break; // TODO or exit_failure??
            }

            client_array_accept_clients(executors_client_array, executors_socket_fd, &read_fds, err);
            if (err->error_code != NO_ERROR)
            {
                CLEAR_ERR(err); // TODO handle failed connection from executor
            }

            for (size_t i = 0; i < executors_client_array->count && collected < others_count; i++)
            {
                if (FD_ISSET(executors_client_array->clients[i], &read_fds))
                {
                    const QueryResponse* response = parse_query_response(executors_client_array->clients[i], err);
                    if (err->error_code != NO_ERROR)
                    {
                        // TODO send_failure response right away or wait for other executors to send their response?
                    }
                    if (response->error != NULL)
                    {
                        LOG_INTERNAL_ERR("Received failure response from a slave executor");
                        SET_ERR(err, INTERNAL_ERROR, response->error->message, response->error->inner_message);
                        // TODO test it
                        // TODO send_failure response right away or wait for other executors to send their response?
                    }
                    else
                    {
                        hash_table_combine_table_with_response(ht, response, err);
                        if (err->error_code != NO_ERROR)
                        {
                            // TODO send_failure response right away or wait for other executors to send their response?
                        }
                    }
                    collected++;
                }
            }
        }

        LOG("Collected from other nodes\n");

        prepare_and_send_response(client_fd, ht, err);
        if (err->error_code != NO_ERROR)
        {
            LOG_INTERNAL_ERR("Failed to send response to controller");
            // TODO handle failed send to controller
        }
    }

    else
    {
        LOG("This node is slave\n");
        const int main_executor_socket = executors_server_find_or_add_main_socket(main_executors_sockets,
            request->executor->main_ip_address, request->executor->main_port, err);
        if (err->error_code != NO_ERROR)
        {
            LOG_INTERNAL_ERR("Failed to connect to main executor\n");
            // TODO handle failed connection to main
        }
        else
        {
            worker_group_run_request(request, &ht, ht_interface, err);
            prepare_and_send_response(main_executor_socket, ht, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_INTERNAL_ERR("Failed to send response to main executor\n");
                // TODO handle failed send to main
            }
            else
            {
                LOG("Sent results to main\n");
            }
        }
    }

    hash_table_free(ht);
    query_request__free_unpacked(request, NULL);
}
