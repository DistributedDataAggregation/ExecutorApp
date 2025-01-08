/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: query_response.proto */

#ifndef PROTOBUF_C_query_5fresponse_2eproto__INCLUDED
#define PROTOBUF_C_query_5fresponse_2eproto__INCLUDED

#include <protobuf-c/protobuf-c.h>

PROTOBUF_C__BEGIN_DECLS

#if PROTOBUF_C_VERSION_NUMBER < 1003000
# error This file was generated by a newer version of protoc-c which is incompatible with your libprotobuf-c headers. Please update your headers.
#elif 1005000 < PROTOBUF_C_MIN_COMPILER_VERSION
# error This file was generated by an older version of protoc-c which is incompatible with your libprotobuf-c headers. Please regenerate this file with a newer version of protoc-c.
#endif

#include "aggregate.pb-c.h"
#include "result_type.pb-c.h"
#include "error.pb-c.h"

typedef struct QueryResponse QueryResponse;
typedef struct Value Value;
typedef struct PartialResult PartialResult;


/* --- enums --- */


/* --- messages --- */

struct  QueryResponse
{
  ProtobufCMessage base;
  char *guid;
  Error *error;
  size_t n_values;
  Value **values;
};
#define QUERY_RESPONSE__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&query_response__descriptor) \
    , (char *)protobuf_c_empty_string, NULL, 0,NULL }


struct  Value
{
  ProtobufCMessage base;
  char *grouping_value;
  size_t n_results;
  PartialResult **results;
};
#define VALUE__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&value__descriptor) \
    , (char *)protobuf_c_empty_string, 0,NULL }


typedef enum {
  PARTIAL_RESULT__VALUE__NOT_SET = 0,
  PARTIAL_RESULT__VALUE_INT_VALUE = 3,
  PARTIAL_RESULT__VALUE_FLOAT_VALUE = 4,
  PARTIAL_RESULT__VALUE_DOUBLE_VALUE = 5
    PROTOBUF_C__FORCE_ENUM_TO_BE_INT_SIZE(PARTIAL_RESULT__VALUE__CASE)
} PartialResult__ValueCase;

struct  PartialResult
{
  ProtobufCMessage base;
  protobuf_c_boolean is_null;
  ResultType type;
  int64_t count;
  Aggregate function;
  PartialResult__ValueCase value_case;
  union {
    int64_t int_value;
    float float_value;
    double double_value;
  };
};
#define PARTIAL_RESULT__INIT \
 { PROTOBUF_C_MESSAGE_INIT (&partial_result__descriptor) \
    , 0, RESULT_TYPE__UNKNOWN, 0, AGGREGATE__Minimum, PARTIAL_RESULT__VALUE__NOT_SET, {0} }


/* QueryResponse methods */
void   query_response__init
                     (QueryResponse         *message);
size_t query_response__get_packed_size
                     (const QueryResponse   *message);
size_t query_response__pack
                     (const QueryResponse   *message,
                      uint8_t             *out);
size_t query_response__pack_to_buffer
                     (const QueryResponse   *message,
                      ProtobufCBuffer     *buffer);
QueryResponse *
       query_response__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   query_response__free_unpacked
                     (QueryResponse *message,
                      ProtobufCAllocator *allocator);
/* Value methods */
void   value__init
                     (Value         *message);
size_t value__get_packed_size
                     (const Value   *message);
size_t value__pack
                     (const Value   *message,
                      uint8_t             *out);
size_t value__pack_to_buffer
                     (const Value   *message,
                      ProtobufCBuffer     *buffer);
Value *
       value__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   value__free_unpacked
                     (Value *message,
                      ProtobufCAllocator *allocator);
/* PartialResult methods */
void   partial_result__init
                     (PartialResult         *message);
size_t partial_result__get_packed_size
                     (const PartialResult   *message);
size_t partial_result__pack
                     (const PartialResult   *message,
                      uint8_t             *out);
size_t partial_result__pack_to_buffer
                     (const PartialResult   *message,
                      ProtobufCBuffer     *buffer);
PartialResult *
       partial_result__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data);
void   partial_result__free_unpacked
                     (PartialResult *message,
                      ProtobufCAllocator *allocator);
/* --- per-message closures --- */

typedef void (*QueryResponse_Closure)
                 (const QueryResponse *message,
                  void *closure_data);
typedef void (*Value_Closure)
                 (const Value *message,
                  void *closure_data);
typedef void (*PartialResult_Closure)
                 (const PartialResult *message,
                  void *closure_data);

/* --- services --- */


/* --- descriptors --- */

extern const ProtobufCMessageDescriptor query_response__descriptor;
extern const ProtobufCMessageDescriptor value__descriptor;
extern const ProtobufCMessageDescriptor partial_result__descriptor;

PROTOBUF_C__END_DECLS


#endif  /* PROTOBUF_C_query_5fresponse_2eproto__INCLUDED */
