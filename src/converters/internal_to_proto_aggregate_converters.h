//
// Created by karol on 05.01.25.
//

#ifndef INTERNAL_TO_PROTO_AGGREGATE_CONVERTERS_H
#define INTERNAL_TO_PROTO_AGGREGATE_CONVERTERS_H

#include "aggregate.pb-c.h"
#include "aggregate_function.h"
#include "error_handling.h"

AggregateFunction convert_aggregate_function(Aggregate aggregate, ErrorInfo* err);
Aggregate convert_aggregate(AggregateFunction aggregate, ErrorInfo* err);


#endif //INTERNAL_TO_PROTO_AGGREGATE_CONVERTERS_H
