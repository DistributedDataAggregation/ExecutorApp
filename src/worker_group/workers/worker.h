//
// Created by karol on 31.10.24.
//

#ifndef WORKER_H
#define WORKER_H

#include "query_request.pb-c.h"
#include "thread_data.h"

void* compute_on_thread(void* arg);

#endif //WORKER_H
