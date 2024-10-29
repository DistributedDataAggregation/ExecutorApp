//
// Created by karol on 29.10.24.
//

#include "request.h"

#include <stdlib.h>

#include "results.h"

void free_request(Request* request) {
    for(int i=0; i<request->files_count; i++) {
        free(request->files[i]);
    }

    for(int i=0; i<request->grouping_columns_count; i++) {
        free(request->grouping_columns[i]);
    }

    for(int i=0; i<request->aggregation_columns_count; i++) {
        free(request->aggregations[i].column);
    }


    free(request->files);
    free(request->grouping_columns);
    free(request->aggregations);

    free(request);
}
