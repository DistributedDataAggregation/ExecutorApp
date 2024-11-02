//
// Created by karol on 02.11.24.
//

#ifndef WORKER_GROUP_H
#define WORKER_GROUP_H

#include "hash_table.h"
#include "query_request.pb-c.h"

HashTable** run_request_on_worker_group(const QueryRequest* request);

#endif //WORKER_GROUP_H
