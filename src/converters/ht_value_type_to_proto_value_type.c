//
// Created by karol on 19.01.25.
//


#include "ht_value_type_to_proto_value_type.h"

ResultType convert_ht_value_type_to_result_type(const HashTableValueType ht_type) {
    switch (ht_type) {
        case HASH_TABLE_INT:
            return RESULT_TYPE__INT;
        case HASH_TABLE_FLOAT:
            return  RESULT_TYPE__FLOAT;
        break;
        case HASH_TABLE_DOUBLE:
            return  RESULT_TYPE__DOUBLE;
        break;
        default:
            return RESULT_TYPE__UNKNOWN;
    }
}