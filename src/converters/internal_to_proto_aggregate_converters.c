//
// Created by karol on 05.01.25.
//
#include "internal_to_proto_aggregate_converters.h"

AggregateFunction convert_aggregate_function(Aggregate aggregate, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return UNKNOWN;
    }

    switch (aggregate)
    {
    case AGGREGATE__Minimum:
        return MIN;
    case AGGREGATE__Maximum:
        return MAX;
    case AGGREGATE__Average:
        return AVG;
    case AGGREGATE__Median:
        return MEDIAN;
    default:
        LOG_INTERNAL_ERR("Unsupported aggregate function");
        SET_ERR(err, INTERNAL_ERROR, "Unsupported aggregate function", "");
        return UNKNOWN;
    }
}

Aggregate convert_aggregate(AggregateFunction aggregate, ErrorInfo* err)
{
    if (err == NULL)
    {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return UNKNOWN;
    }

    switch (aggregate)
    {
    case MIN:
        return AGGREGATE__Minimum;
    case MAX:
        return AGGREGATE__Maximum;
    case AVG:
        return AGGREGATE__Average;
    case MEDIAN:
        return AGGREGATE__Median;
    default:
        LOG_INTERNAL_ERR("Unsupported aggregate function");
        SET_ERR(err, INTERNAL_ERROR, "Unsupported AggregateFunction",
                "Unsupported AggregateFunction enum value for mapping to proto Aggreagate\n");
        return AGGREGATE__Unknown;
    }
}
