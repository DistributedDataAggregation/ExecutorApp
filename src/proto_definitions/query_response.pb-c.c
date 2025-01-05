/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: query_response.proto */

/* Do not generate deprecated warnings for self */
#ifndef PROTOBUF_C__NO_DEPRECATED
#define PROTOBUF_C__NO_DEPRECATED
#endif

#include "query_response.pb-c.h"
void   query_response__init
                     (QueryResponse         *message)
{
  static const QueryResponse init_value = QUERY_RESPONSE__INIT;
  *message = init_value;
}
size_t query_response__get_packed_size
                     (const QueryResponse *message)
{
  assert(message->base.descriptor == &query_response__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t query_response__pack
                     (const QueryResponse *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &query_response__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t query_response__pack_to_buffer
                     (const QueryResponse *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &query_response__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
QueryResponse *
       query_response__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (QueryResponse *)
     protobuf_c_message_unpack (&query_response__descriptor,
                                allocator, len, data);
}
void   query_response__free_unpacked
                     (QueryResponse *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &query_response__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
void   value__init
                     (Value         *message)
{
  static const Value init_value = VALUE__INIT;
  *message = init_value;
}
size_t value__get_packed_size
                     (const Value *message)
{
  assert(message->base.descriptor == &value__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t value__pack
                     (const Value *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &value__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t value__pack_to_buffer
                     (const Value *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &value__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
Value *
       value__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (Value *)
     protobuf_c_message_unpack (&value__descriptor,
                                allocator, len, data);
}
void   value__free_unpacked
                     (Value *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &value__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
void   partial_result__init
                     (PartialResult         *message)
{
  static const PartialResult init_value = PARTIAL_RESULT__INIT;
  *message = init_value;
}
size_t partial_result__get_packed_size
                     (const PartialResult *message)
{
  assert(message->base.descriptor == &partial_result__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t partial_result__pack
                     (const PartialResult *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &partial_result__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t partial_result__pack_to_buffer
                     (const PartialResult *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &partial_result__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
PartialResult *
       partial_result__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (PartialResult *)
     protobuf_c_message_unpack (&partial_result__descriptor,
                                allocator, len, data);
}
void   partial_result__free_unpacked
                     (PartialResult *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &partial_result__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
void   error__init
                     (Error         *message)
{
  static const Error init_value = ERROR__INIT;
  *message = init_value;
}
size_t error__get_packed_size
                     (const Error *message)
{
  assert(message->base.descriptor == &error__descriptor);
  return protobuf_c_message_get_packed_size ((const ProtobufCMessage*)(message));
}
size_t error__pack
                     (const Error *message,
                      uint8_t       *out)
{
  assert(message->base.descriptor == &error__descriptor);
  return protobuf_c_message_pack ((const ProtobufCMessage*)message, out);
}
size_t error__pack_to_buffer
                     (const Error *message,
                      ProtobufCBuffer *buffer)
{
  assert(message->base.descriptor == &error__descriptor);
  return protobuf_c_message_pack_to_buffer ((const ProtobufCMessage*)message, buffer);
}
Error *
       error__unpack
                     (ProtobufCAllocator  *allocator,
                      size_t               len,
                      const uint8_t       *data)
{
  return (Error *)
     protobuf_c_message_unpack (&error__descriptor,
                                allocator, len, data);
}
void   error__free_unpacked
                     (Error *message,
                      ProtobufCAllocator *allocator)
{
  if(!message)
    return;
  assert(message->base.descriptor == &error__descriptor);
  protobuf_c_message_free_unpacked ((ProtobufCMessage*)message, allocator);
}
static const ProtobufCFieldDescriptor query_response__field_descriptors[3] =
{
  {
    "guid",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(QueryResponse, guid),
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
    offsetof(QueryResponse, error),
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
    offsetof(QueryResponse, n_values),
    offsetof(QueryResponse, values),
    &value__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned query_response__field_indices_by_name[] = {
  1,   /* field[1] = error */
  0,   /* field[0] = guid */
  2,   /* field[2] = values */
};
static const ProtobufCIntRange query_response__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 3 }
};
const ProtobufCMessageDescriptor query_response__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "QueryResponse",
  "QueryResponse",
  "QueryResponse",
  "",
  sizeof(QueryResponse),
  3,
  query_response__field_descriptors,
  query_response__field_indices_by_name,
  1,  query_response__number_ranges,
  (ProtobufCMessageInit) query_response__init,
  NULL,NULL,NULL    /* reserved[123] */
};
static const ProtobufCFieldDescriptor value__field_descriptors[2] =
{
  {
    "grouping_value",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(Value, grouping_value),
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
    offsetof(Value, n_results),
    offsetof(Value, results),
    &partial_result__descriptor,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned value__field_indices_by_name[] = {
  0,   /* field[0] = grouping_value */
  1,   /* field[1] = results */
};
static const ProtobufCIntRange value__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 2 }
};
const ProtobufCMessageDescriptor value__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "Value",
  "Value",
  "Value",
  "",
  sizeof(Value),
  2,
  value__field_descriptors,
  value__field_indices_by_name,
  1,  value__number_ranges,
  (ProtobufCMessageInit) value__init,
  NULL,NULL,NULL    /* reserved[123] */
};
static const ProtobufCFieldDescriptor partial_result__field_descriptors[3] =
{
  {
    "is_null",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_BOOL,
    0,   /* quantifier_offset */
    offsetof(PartialResult, is_null),
    NULL,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "value",
    2,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_INT64,
    0,   /* quantifier_offset */
    offsetof(PartialResult, value),
    NULL,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "count",
    3,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_INT64,
    0,   /* quantifier_offset */
    offsetof(PartialResult, count),
    NULL,
    NULL,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned partial_result__field_indices_by_name[] = {
  2,   /* field[2] = count */
  0,   /* field[0] = is_null */
  1,   /* field[1] = value */
};
static const ProtobufCIntRange partial_result__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 3 }
};
const ProtobufCMessageDescriptor partial_result__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "PartialResult",
  "PartialResult",
  "PartialResult",
  "",
  sizeof(PartialResult),
  3,
  partial_result__field_descriptors,
  partial_result__field_indices_by_name,
  1,  partial_result__number_ranges,
  (ProtobufCMessageInit) partial_result__init,
  NULL,NULL,NULL    /* reserved[123] */
};
static const ProtobufCFieldDescriptor error__field_descriptors[2] =
{
  {
    "message",
    1,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(Error, message),
    NULL,
    &protobuf_c_empty_string,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
  {
    "inner_message",
    2,
    PROTOBUF_C_LABEL_NONE,
    PROTOBUF_C_TYPE_STRING,
    0,   /* quantifier_offset */
    offsetof(Error, inner_message),
    NULL,
    &protobuf_c_empty_string,
    0,             /* flags */
    0,NULL,NULL    /* reserved1,reserved2, etc */
  },
};
static const unsigned error__field_indices_by_name[] = {
  1,   /* field[1] = inner_message */
  0,   /* field[0] = message */
};
static const ProtobufCIntRange error__number_ranges[1 + 1] =
{
  { 1, 0 },
  { 0, 2 }
};
const ProtobufCMessageDescriptor error__descriptor =
{
  PROTOBUF_C__MESSAGE_DESCRIPTOR_MAGIC,
  "Error",
  "Error",
  "Error",
  "",
  sizeof(Error),
  2,
  error__field_descriptors,
  error__field_indices_by_name,
  1,  error__number_ranges,
  (ProtobufCMessageInit) error__init,
  NULL,NULL,NULL    /* reserved[123] */
};
