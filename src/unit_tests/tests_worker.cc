#include "worker.h"

#include <gtest/gtest.h>
#include <cstring>
#include <arrow-glib/arrow-glib.h>


TEST(WorkerTests, CalculateNewColumnIndices_UniqueColumns)
{
    gint old_column_indices[] = {1, 2, 3};
    int new_column_indices[3];
    worker_calculate_new_column_indices(new_column_indices, old_column_indices, 3);

    EXPECT_EQ(new_column_indices[0], 0);
    EXPECT_EQ(new_column_indices[1], 1);
    EXPECT_EQ(new_column_indices[2], 2);
}

TEST(WorkerTests, CalculateNewColumnIndices_RepeatedColumns)
{
    gint old_column_indices[] = {1, 2, 1};
    int new_column_indices[3];
    worker_calculate_new_column_indices(new_column_indices, old_column_indices, 3);

    EXPECT_EQ(new_column_indices[0], 0);
    EXPECT_EQ(new_column_indices[1], 1);
    EXPECT_EQ(new_column_indices[2], 0);
}

TEST(WorkerTests, CalculateNewColumnIndices_EmptyInput)
{
    gint old_column_indices[] = {};
    int new_column_indices[0];
    worker_calculate_new_column_indices(new_column_indices, old_column_indices, 0);

    SUCCEED();
}

//
// TEST(WorkerTests, ConstructGroupingString) {
//     GArrowArray* grouping_arrays[2];
//     ColumnDataType types[2] = {COLUMN_DATA_TYPE_INT32, COLUMN_DATA_TYPE_STRING};
//
//
//     GArrowBuffer* int32_data = nullptr;
//     GArrowBuffer* int32_null_bitmap = nullptr;
//     gint64 int32_null_count = 0;
//
//     GArrowBuffer* string_data = nullptr;
//     GArrowBuffer* string_null_bitmap = nullptr;
//     GArrowBuffer* string_offsets = nullptr;
//     gint64 string_null_count = 0;
//
//
//     grouping_arrays[0] = GARROW_ARRAY(garrow_int32_array_new(3, int32_data, int32_null_bitmap, int32_null_count));
//     grouping_arrays[1] = GARROW_ARRAY(garrow_string_array_new(3, string_data, string_null_bitmap, string_offsets, string_null_count));
//
//     // Ustawienie wartości w tablicach
//     garrow_int32_array_set_value(GARROW_INT32_ARRAY(grouping_arrays[0]), 0, 42);
//     garrow_string_array_set_string(GARROW_STRING_ARRAY(grouping_arrays[1]), 0, "test");
//
//     // Wywołanie funkcji
//     char* result = construct_grouping_string(2, grouping_arrays, 0, types);
//
//     EXPECT_STREQ(result, "42test");
//
//     free(result);
//     g_object_unref(grouping_arrays[0]);
//     g_object_unref(grouping_arrays[1]);
// }
//
// TEST(WorkerTests, GetGroupingString_Int32) {
//     GArrowArray* array;
//     GArrowBuffer* int32_data = nullptr; // Zainicjalizuj odpowiedni bufor danych
//     GArrowBuffer* int32_null_bitmap = nullptr;
//     gint64 int32_null_count = 0;
//
//     array = GARROW_ARRAY(garrow_int32_array_new(3, int32_data, int32_null_bitmap, int32_null_count));
//     garrow_int32_array_set_value(GARROW_INT32_ARRAY(array), 0, 42);
//
//     char* result = get_grouping_string(array, COLUMN_DATA_TYPE_INT32, 0);
//
//     EXPECT_STREQ(result, "42");
//
//     free(result);
//     g_object_unref(array);
// }
//
// TEST(WorkerTests, GetGroupingString_String) {
//     GArrowArray* array;
//     GArrowBuffer* string_data = nullptr; // Zainicjalizuj odpowiedni bufor danych
//     GArrowBuffer* string_null_bitmap = nullptr;
//     GArrowBuffer* string_offsets = nullptr;
//     gint64 string_null_count = 0;
//
//     array = GARROW_ARRAY(garrow_string_array_new(3, string_data, string_null_bitmap, string_offsets, string_null_count));
//     garrow_string_array_set_string(GARROW_STRING_ARRAY(array), 0, "test");
//
//     char* result = get_grouping_string(array, COLUMN_DATA_TYPE_STRING, 0);
//
//     EXPECT_STREQ(result, "test");
//
//     free(result);
//     g_object_unref(array);
// }
//
// TEST(WorkerTests, GetGroupingString_UnknownType) {
//     GArrowArray* array;
//     GArrowBuffer* int32_data = nullptr; // Zainicjalizuj odpowiedni bufor danych
//     GArrowBuffer* int32_null_bitmap = nullptr;
//     gint64 int32_null_count = 0;
//
//     array = GARROW_ARRAY(garrow_int32_array_new(3, int32_data, int32_null_bitmap, int32_null_count));
//
//     char* result = get_grouping_string(array, COLUMN_DATA_TYPE_UNKNOWN, 0);
//
//     EXPECT_EQ(result, nullptr);
//
//     g_object_unref(array);
// }
//
// TEST(WorkerTests, GetHashTableValue_Int32_Min) {
//     GArrowArray* array;
//     GArrowBuffer* int32_data = nullptr; // Zainicjalizuj odpowiedni bufor danych
//     GArrowBuffer* int32_null_bitmap = nullptr;
//     gint64 int32_null_count = 0;
//
//     array = GARROW_ARRAY(garrow_int32_array_new(3, int32_data, int32_null_bitmap, int32_null_count));
//     garrow_int32_array_set_value(GARROW_INT32_ARRAY(array), 0, 42);
//
//     HashTableValue value = get_hash_table_value(array, 0, COLUMN_DATA_TYPE_INT32, MIN);
//
//     EXPECT_EQ(value.value, 42);
//     EXPECT_EQ(value.aggregate_function, MIN);
//
//     g_object_unref(array);
// }
//
// TEST(WorkerTests, GetHashTableValue_UnknownType) {
//     GArrowArray* array;
//     GArrowBuffer* int32_data = nullptr; // Zainicjalizuj odpowiedni bufor danych
//     GArrowBuffer* int32_null_bitmap = nullptr;
//     gint64 int32_null_count = 0;
//
//     array = GARROW_ARRAY(garrow_int32_array_new(3, int32_data, int32_null_bitmap, int32_null_count));
//
//     HashTableValue value = get_hash_table_value(array, 0, COLUMN_DATA_TYPE_UNKNOWN, MIN);
//
//     EXPECT_EQ(value.aggregate_function, UNKNOWN);
//
//     g_object_unref(array);
// }
