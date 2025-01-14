/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: query_result.proto */

/* Do not generate deprecated warnings for self */
#ifndef PROTOBUF_C__NO_DEPRECATED
#define PROTOBUF_C__NO_DEPRECATED
#endif

#include "query_result.pb-c.h"
void   query_result__init
                     (QueryResult         *message)
{
  static const QueryResult init_value = QUERY_RESULT__INIT;
  *message = init_value;
}
size_t query_result__get_packed_size
                     (const QueryResult *message)
{
  assert(message->base.descriptor == &query_result__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t query_result__pack
                     (const QueryResult *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &query_result__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t query_result__pack_to_buffer
                     (const QueryResult *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &query_result__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
QueryResult *
       query_result__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (QueryResult *)
     protobuf_c_message_unpack (&query_result__descriptor,
                                allocator, len, data);
}
void   query_result__free_unpacked
                     (QueryResult *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &query_result__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
void   result_value__init
                     (ResultValue         *message)
{
  static const ResultValue init_value = RESULT_VALUE__INIT;
  *message = init_value;
}
size_t result_value__get_packed_size
                     (const ResultValue *message)
{
  assert(message->base.descriptor == &result_value__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t result_value__pack
                     (const ResultValue *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &result_value__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t result_value__pack_to_buffer
                     (const ResultValue *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &result_value__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
ResultValue *
       result_value__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (ResultValue *)
     protobuf_c_message_unpack (&result_value__descriptor,
                                allocator, len, data);
}
void   result_value__free_unpacked
                     (ResultValue *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &result_value__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
void   combined_result__init
                     (CombinedResult         *message)
{
  static const CombinedResult init_value = COMBINED_RESULT__INIT;
  *message = init_value;
}
size_t combined_result__get_packed_size
                     (const CombinedResult *message)
{
  assert(message->base.descriptor == &combined_result__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t combined_result__pack
                     (const CombinedResult *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &combined_result__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t combined_result__pack_to_buffer
                     (const CombinedResult *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &combined_result__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
CombinedResult *
       combined_result__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (CombinedResult *)
     protobuf_c_message_unpack (&combined_result__descriptor,
                                allocator, len, data);
}
void   combined_result__free_unpacked
                     (CombinedResult *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &combined_result__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
static const ProtobufCFieldDescriptor query_result__field_descriptors[3] =
{
  {
    "guid",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(QueryResult, guid),
    NULL,
    &protobuf_c_empty_string,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "error",
    2,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_MESSAGE,
    0,   /* quantifier_offset */
    offsetof(QueryResult, error),
    &error__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "values",
    3,
    PROTOBUF_C_LABEL_REPEATED,
    PROTOBUF_C_TYPE_MESSAGE,
    offsetof(QueryResult, n_values),
    offsetof(QueryResult, values),
    &result_value__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned query_result__field_indices_by_name[] = {
  1,   /* field[1] = error */
  0,   /* field[0] = guid */
  2,   /* field[2] = values */
};
static const ProtobufCIntRange query_result__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 3 }
};
const ProtobufCMessageDescriptor query_result__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "QueryResult",
  "QueryResult",
  "QueryResult",
  "",
  sizeof(QueryResult),
  3,
  query_result__field_descriptors,
  query_result__field_indices_by_name,
  1,  query_result__number_ranges,
  (ProtobufCMessageInit) query_result__init,
  NULL,NULL,NULL    /* reserved[123] */
};
static const ProtobufCFieldDescriptor result_value__field_descriptors[2] =
{
  {
    "grouping_value",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(ResultValue, grouping_value),
    NULL,
    &protobuf_c_empty_string,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "results",
    2,
    PROTOBUF_C_LABEL_REPEATED,
    PROTOBUF_C_TYPE_MESSAGE,
    offsetof(ResultValue, n_results),
    offsetof(ResultValue, results),
    &combined_result__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned result_value__field_indices_by_name[] = {
  0,   /* field[0] = grouping_value */
  1,   /* field[1] = results */
};
static const ProtobufCIntRange result_value__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 2 }
};
const ProtobufCMessageDescriptor result_value__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "ResultValue",
  "ResultValue",
  "ResultValue",
  "",
  sizeof(ResultValue),
  2,
  result_value__field_descriptors,
  result_value__field_indices_by_name,
  1,  result_value__number_ranges,
  (ProtobufCMessageInit) result_value__init,
  NULL,NULL,NULL    /* reserved[123] */
};
static const ProtobufCFieldDescriptor combined_result__field_descriptors[6] =
{
  {
    "is_null",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_BOOL,
    0,   /* quantifier_offset */
    offsetof(CombinedResult, is_null),
    NULL,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "type",
    2,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_ENUM,
    0,   /* quantifier_offset */
    offsetof(CombinedResult, type),
    &result_type__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "int_value",
    3,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_INT64,
    offsetof(CombinedResult, value_case),
    offsetof(CombinedResult, int_value),
    NULL,
    NULL,
    PROTOBUF_C_FIELD_FLAG_ONEOF,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "float_value",
    4,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_FLOAT,
    offsetof(CombinedResult, value_case),
    offsetof(CombinedResult, float_value),
    NULL,
    NULL,
    PROTOBUF_C_FIELD_FLAG_ONEOF,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "double_value",
    5,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_DOUBLE,
    offsetof(CombinedResult, value_case),
    offsetof(CombinedResult, double_value),
    NULL,
    NULL,
    PROTOBUF_C_FIELD_FLAG_ONEOF,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "function",
    6,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_ENUM,
    0,   /* quantifier_offset */
    offsetof(CombinedResult, function),
    &aggregate__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned combined_result__field_indices_by_name[] = {
  4,   /* field[4] = double_value */
  3,   /* field[3] = float_value */
  5,   /* field[5] = function */
  2,   /* field[2] = int_value */
  0,   /* field[0] = is_null */
  1,   /* field[1] = type */
};
static const ProtobufCIntRange combined_result__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 6 }
};
const ProtobufCMessageDescriptor combined_result__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "CombinedResult",
  "CombinedResult",
  "CombinedResult",
  "",
  sizeof(CombinedResult),
  6,
  combined_result__field_descriptors,
  combined_result__field_indices_by_name,
  1,  combined_result__number_ranges,
  (ProtobufCMessageInit) combined_result__init,
  NULL,NULL,NULL    /* reserved[123] */
};
