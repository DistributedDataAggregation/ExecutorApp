#include "worker_group.h"


#include <gtest/gtest.h>
#include <cstring>

TEST(MapArrowDataTypeTest, Int32Type)
{
    ErrorInfo error_info = {0};

    GArrowInt32DataType* int32_data_type_specific = garrow_int32_data_type_new();
    ASSERT_NE(int32_data_type_specific, nullptr);


    GArrowDataType* int32_data_type = GARROW_DATA_TYPE(int32_data_type_specific);
    ASSERT_NE(int32_data_type, nullptr);

    ColumnDataType result = worker_group_map_arrow_data_type(int32_data_type, &error_info);
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

TEST(MapArrowDataTypeTest, UnknownType)
{
    ErrorInfo error_info = {0};

    GArrowDataType* unknown_data_type = nullptr;
    ColumnDataType result = worker_group_map_arrow_data_type(unknown_data_type, &error_info);
    EXPECT_EQ(result, COLUMN_DATA_TYPE_UNKNOWN);
}


TEST(MapArrowDataTypeTest, StringType)
{
    ErrorInfo error_info = {0};

    GArrowDataType* string_data_type = GARROW_DATA_TYPE(garrow_string_data_type_new());
    ASSERT_NE(string_data_type, nullptr);

    ColumnDataType result = worker_group_map_arrow_data_type(string_data_type, &error_info);
    EXPECT_EQ(result, COLUMN_DATA_TYPE_STRING);

    g_object_unref(string_data_type);
}


TEST(WorkerGroupTests, GetColumnsIndices)
{
    ErrorInfo error_info = {0};

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

    worker_group_get_columns_indices(&request, grouping_indices, select_indices, &error_info);
    EXPECT_NE(error_info.error_code, NO_ERROR);

    free(request.group_columns[0]);
    free(request.group_columns);
    free(request.select[0]->column);
    free(request.select[0]);
    free(request.select);
}

TEST(WorkerGroupTests, GetRowGroupRanges)
{
    ErrorInfo error_info = {0};
    char* file_name = strdup("test.parquet");
    char* file_names[] = {file_name};
    RowGroupsRange** ranges = worker_group_get_row_group_ranges(1, file_names, 2, &error_info);

    free(file_name);
    EXPECT_EQ(ranges, nullptr);
}

TEST(WorkerGroupTests, FreeRowGroupRanges)
{
    RowGroupsRange** ranges = (RowGroupsRange**)malloc(sizeof(RowGroupsRange*) * 2);
    ranges[0] = (RowGroupsRange*)malloc(sizeof(RowGroupsRange));
    ranges[1] = (RowGroupsRange*)malloc(sizeof(RowGroupsRange));

    worker_group_free_row_group_ranges(ranges, 2);


    SUCCEED();
}

TEST(WorkerGroupTests, RunRequest_NullRequest)
{
    ErrorInfo error_info = {0};
    HashTable* hash_table = nullptr;
    worker_group_run_request(nullptr, &hash_table, nullptr, &error_info);
    EXPECT_NE(error_info.error_code, NO_ERROR);
}

TEST(WorkerGroupTests, GetThreadData_NullRequest)
{
    ErrorInfo error_info = {0};
    ThreadData* thread_data = worker_group_get_thread_data(nullptr, 0, 1, nullptr, nullptr,
                                                           nullptr, nullptr, nullptr, nullptr, 0, nulltpr, &error_info);
    EXPECT_EQ(thread_data, nullptr);
}

TEST(WorkerGroupTests, FreeThreadData_NullInput)
{
    worker_group_free_thread_data(nullptr);
    SUCCEED();
}
