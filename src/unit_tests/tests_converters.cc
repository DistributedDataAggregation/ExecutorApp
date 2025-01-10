extern "C" {
#include "hash_table_to_query_response_converter.h"
}

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>

TEST(HashTableToQueryResponseTest, NonEmptyHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_create(10, 100,&error_info);
    ASSERT_NE(ht, nullptr);
    EXPECT_EQ(ht->entries_count, 0);

    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry == nullptr)
    {
        hash_table_free(ht);
        return;
    }
    entry->key = strdup("key1");
    if (entry->key == nullptr)
    {
        free(entry);
        hash_table_free(ht);
        return;
    }
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    if (entry->values == nullptr)
    {
        free(entry->key);
        free(entry);
        hash_table_free(ht);
        return;
    }
    entry->values[0].value = 42;
    entry->values[0].count = 1;
    entry->values[0].type = HASH_TABLE_INT;
    entry->values[0].aggregate_function = AVG;
    entry->next = nullptr;

    hash_table_insert(ht, entry, &error_info);
    if (error_info.error_code != NO_ERROR)
    {
        free(entry->values);
        free(entry->key);
        free(entry);
        hash_table_free(ht);
        return;
    }
    QueryResponse* response = convert_hash_table_to_query_response(ht, &error_info);
    if (response == nullptr)
    {
        hash_table_free(ht);
        return;
    }

    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->n_values, 1);
    ASSERT_NE(response->values, nullptr);


    EXPECT_STREQ(response->values[0]->grouping_value, "key1");
    EXPECT_EQ(response->values[0]->n_results, 1);
    EXPECT_EQ(response->values[0]->results[0]->int_value, 42);
    EXPECT_EQ(response->values[0]->results[0]->count, 1);

    for (int i = 0; i < response->n_values; i++)
    {
        free(response->values[i]->grouping_value);
        for (int j = 0; j < response->values[i]->n_results; j++)
        {
            free(response->values[i]->results[j]);
        }
        free(response->values[i]->results);
        free(response->values[i]);
    }
    free(response->values);
    free(response);

    hash_table_free(ht);
}
