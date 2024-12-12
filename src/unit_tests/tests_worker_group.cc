
#include "worker_group.h"


#include <gtest/gtest.h>
#include <cstring>

TEST(MapArrowDataTypeTest, Int32Type) {

    GArrowInt32DataType* int32_data_type_specific = garrow_int32_data_type_new();
    ASSERT_NE(int32_data_type_specific, nullptr);


    GArrowDataType* int32_data_type = GARROW_DATA_TYPE(int32_data_type_specific);
    ASSERT_NE(int32_data_type, nullptr);

    ColumnDataType result = worker_group_map_arrow_data_type(int32_data_type);
    EXPECT_EQ(result, COLUMN_DATA_TYPE_INT32);

    g_object_unref(int32_data_type);
}

// TEST(MapArrowDataTypeTest, Int64Type) {
//     GArrowDataType* int64_data_type = garrow_int64_data_type_new();
//     ASSERT_NE(int64_data_type, nullptr);
//
//     ColumnDataType result = worker_group_map_arrow_data_type(int64_data_type);
//     EXPECT_EQ(result, COLUMN_DATA_TYPE_INT64);
//
//     g_object_unref(int64_data_type);
// }
//
// TEST(MapArrowDataTypeTest, StringType) {
//     GArrowDataType* string_data_type = garrow_string_data_type_new();
//     ASSERT_NE(string_data_type, nullptr);
//
//     ColumnDataType result = worker_group_map_arrow_data_type(string_data_type);
//     EXPECT_EQ(result, COLUMN_DATA_TYPE_STRING);
//
//     g_object_unref(string_data_type);
// }

TEST(MapArrowDataTypeTest, UnknownType) {
    GArrowDataType* unknown_data_type = nullptr;
    ColumnDataType result = worker_group_map_arrow_data_type(unknown_data_type);
    EXPECT_EQ(result, COLUMN_DATA_TYPE_UNKNOWN);
}



TEST(MapArrowDataTypeTest, StringType) {
    GArrowDataType* string_data_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
    ASSERT_NE(string_data_type, nullptr);

    ColumnDataType result = worker_group_map_arrow_data_type(string_data_type);
    EXPECT_EQ(result, COLUMN_DATA_TYPE_STRING);

    g_object_unref(string_data_type);
}



TEST(WorkerGroupTests, GetColumnsIndices) {
    QueryRequest request = QUERY_REQUEST__INIT;
    request.n_group_columns = 1;
    request.group_columns = (char**)malloc(sizeof(char*));
    request.group_columns[0] = strdup("column1");

    request.n_select = 1;
    request.select = (Select**)malloc(sizeof(Select*));
    request.select[0] = (Select*)malloc(sizeof(Select));
    request.select[0]->column = strdup("column2");

    int grouping_indices[1];
    int select_indices[1];

    EXPECT_FALSE(worker_group_get_columns_indices(&request, grouping_indices, select_indices));

    free(request.group_columns[0]);
    free(request.group_columns);
    free(request.select[0]->column);
    free(request.select[0]);
    free(request.select);
}

TEST(WorkerGroupTests, GetRowGroupRanges) {
    char* file_names[] = {"test.parquet"};
    RowGroupsRange** ranges = worker_group_get_row_group_ranges(1, file_names, 2);

    EXPECT_EQ(ranges, nullptr);
}

TEST(WorkerGroupTests, FreeRowGroupRanges) {
    RowGroupsRange** ranges = (RowGroupsRange**)malloc(sizeof(RowGroupsRange*) * 2);
    ranges[0] = (RowGroupsRange*)malloc(sizeof(RowGroupsRange));
    ranges[1] = (RowGroupsRange*)malloc(sizeof(RowGroupsRange));

    worker_group_free_row_group_ranges(ranges, 2);


    SUCCEED();
}

TEST(WorkerGroupTests, RunRequest_NullRequest) {
    HashTable* hash_table = nullptr;
    int result = worker_group_run_request(nullptr, hash_table);
    EXPECT_EQ(result, -1);
}

TEST(WorkerGroupTests, GetThreadData_NullRequest) {
    ThreadData* thread_data = worker_group_get_thread_data(nullptr, 0, 1, nullptr, nullptr, nullptr, nullptr, nullptr);
    EXPECT_EQ(thread_data, nullptr);
}

TEST(WorkerGroupTests, FreeThreadData_NullInput) {
    worker_group_free_thread_data(nullptr);
    SUCCEED();
}