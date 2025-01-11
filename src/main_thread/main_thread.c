//
// Created by karol on 26.10.24.
//

#include <google/protobuf-c/protobuf-c.h>
#include <parquet-glib/arrow-file-reader.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <unistd.h>
#include "stdbool.h"
#include "client_array.h"
#include "error_handling.h"
#include "executors_server.h"
#include "main_thread.h"

#include "hash_table_interface.h"
#include "query_request.pb-c.h"
#include "query_response.pb-c.h"
#include "request_protocol/request_protocol.h"
#include "socket_utilities.h"
#include "worker_group.h"
#include "logging.h"

#define INITIAL_SIZE 10
#define MAX_ITERS 100

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
                                                                      true, true, controllers_port, &error_info);
    const int executors_socket_fd = create_and_listen_on_tcp_socket("0.0.0.0",
                                                                    true, true, executors_port, &error_info);
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
                    if (error_info.error_code == SOCKET_CLOSED || error_info.error_code == EAGAIN
                        || error_info.error_code == ECONNRESET || error_info.error_code == EPIPE)
                    {
                        LOG("Removing client: %d\n", controllers_client_array.clients[i]);
                        client_array_remove_client(&controllers_client_array, i);
                        i--;
                    }
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
    //HashTableInterface* ht_interface = create_default_hash_table_interface();
    HashTableInterface* ht_interface = create_optimized_hash_table_interface();

    if (request->executor->is_current_node_main)
    {
        LOG("This node is main\n");
        worker_group_run_request(request, &ht, ht_interface, err);

        if (err->error_code != NO_ERROR)
        {
            // TODO send_failure response right away or wait for other executors to send their response?
            // if we wait, we need to initialize the hash table because it may be null
        }

        int collected = 0, iters = 0;
        const int others_count = request->executor->executors_count - 1;

        LOG("Finished computing\n");
        while (collected < others_count && iters < MAX_ITERS)
        {
            iters++;

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
                        if (err->error_code == SOCKET_CLOSED || err->error_code == EAGAIN)
                        {
                            LOG("Removing client executor: %d\n", executors_client_array->clients[i]);
                            client_array_remove_client(executors_client_array, i);
                            i--;
                            CLEAR_ERR(err);
                        }
                        // TODO send_failure response right away or wait for other executors to send their response?
                    }
                    else
                    {
                        if (strcmp(response->guid, request->guid) == 0)
                        {
                            if (response->error != NULL)
                            {
                                LOG_INTERNAL_ERR("Received failure response from a slave executor");
                                SET_ERR(err, INTERNAL_ERROR, response->error->message, response->error->inner_message);
                                // TODO test it
                                // TODO send_failure response right away or wait for other executors to send their response?
                            }
                            else
                            {
                                LOG("Combining response for query of id: %s", response->guid);
                                ht_interface->combine_with_response(ht, response, err);
                                if (err->error_code != NO_ERROR)
                                {
                                    LOG("ERROR Combining response for query of id: %s", response->guid);
                                    LOG_INTERNAL_ERR("Combining response for query");
                                    // TODO send_failure response right away or wait for other executors to send their response?
                                }
                            }
                            collected++;
                        }
                    }
                }
            }
        }

        if (iters == MAX_ITERS)
        {
            LOG_INTERNAL_ERR("Failed to collect from other executors");
            SET_ERR(err, INTERNAL_ERROR, "Failed to collect from other executors", "");
        }

        prepare_and_send_result(client_fd, request->guid, ht_interface, ht, err);
        if (err->error_code != NO_ERROR)
        {
            LOG_INTERNAL_ERR("Failed to send response to controller");
            // TODO handle failed send to controller, retry if EAGAIN lub EWOULDBLOCK? (closing in main thread)
        }
        LOG("Sent results to controller\n");
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
            CLEAR_ERR(err);
        }
        else
        {
            worker_group_run_request(request, &ht, ht_interface, err);
            prepare_and_send_response(main_executor_socket, request->guid, ht_interface, ht, err);
            if (err->error_code != NO_ERROR)
            {
                LOG_INTERNAL_ERR("Failed to send response to main executor\n");
                if (err->error_code == ECONNRESET || err->error_code == EPIPE)
                {
                    LOG("Removing main socket %d from main_executors\n", main_executor_socket);
                    executors_server_remove_main_socket(main_executors_sockets, main_executor_socket);
                    CLEAR_ERR(err);
                }
                // TODO handle failed send to main, retry if EAGAIN lub EWOULDBLOCK, close if EPIPE, ECONNRESET?
            }
        }
    }

    ht_interface->free(ht);
    free(ht_interface);
    query_request__free_unpacked(request, NULL);
}
