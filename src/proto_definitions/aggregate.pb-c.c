/* Generated by the protocol buffer compiler.  DO NOT EDIT! */
/* Generated from: aggregate.proto */

/* Do not generate deprecated warnings for self */
#ifndef PROTOBUF_C__NO_DEPRECATED
#define PROTOBUF_C__NO_DEPRECATED
#endif

#include "aggregate.pb-c.h"
static const ProtobufCEnumValue aggregate__enum_values_by_number[4] =
{
  { "Minimum", "AGGREGATE__Minimum", 0 },
  { "Maximum", "AGGREGATE__Maximum", 1 },
  { "Average", "AGGREGATE__Average", 2 },
  { "Median", "AGGREGATE__Median", 3 },
};
static const ProtobufCIntRange aggregate__value_ranges[] = {
{0, 0},{0, 4}
};
static const ProtobufCEnumValueIndex aggregate__enum_values_by_name[4] =
{
  { "Average", 2 },
  { "Maximum", 1 },
  { "Median", 3 },
  { "Minimum", 0 },
};
const ProtobufCEnumDescriptor aggregate__descriptor =
{
  PROTOBUF_C__ENUM_DESCRIPTOR_MAGIC,
  "Aggregate",
  "Aggregate",
  "Aggregate",
  "",
  4,
  aggregate__enum_values_by_number,
  4,
  aggregate__enum_values_by_name,
  1,
  aggregate__value_ranges,
  NULL,NULL,NULL,NULL   /* reserved[1234] */
};