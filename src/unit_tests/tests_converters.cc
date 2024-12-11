extern "C" {
#include "hash_table_to_query_response_converter.h"
}
#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>


TEST(HashTableToQueryResponseTest, EmptyHashTable) {
    HashTable* ht = hash_table_create(10);
    ASSERT_NE(ht, nullptr);
    EXPECT_EQ(ht->entries_count, 0);

    QueryResponse* response = convert_hash_table_to_query_response(ht);

    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->n_values, 0);
    EXPECT_EQ(response->values, nullptr);

    free(response);
    hash_table_free(ht);
}

TEST(HashTableToQueryResponseTest, NonEmptyHashTable) {
    HashTable* ht = hash_table_create(10);
    ASSERT_NE(ht, nullptr);
    EXPECT_EQ(ht->entries_count, 0);

    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry->key = strdup("key1");
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry->values[0].value = 42;
    entry->values[0].count = 1;
    entry->values[0].aggregate_function = AVG;
    entry->next = nullptr;

    hash_table_insert(ht, entry);

    QueryResponse* response = convert_hash_table_to_query_response(ht);

    ASSERT_NE(response, nullptr);
    EXPECT_EQ(response->n_values, 1);
    ASSERT_NE(response->values, nullptr);


    EXPECT_STREQ(response->values[0]->grouping_value, "key1");
    EXPECT_EQ(response->values[0]->n_results, 1);
    EXPECT_EQ(response->values[0]->results[0]->value, 42);
    EXPECT_EQ(response->values[0]->results[0]->count, 1);

    for (int i = 0; i < response->n_values; i++) {
        free(response->values[i]->grouping_value);
        for (int j = 0; j < response->values[i]->n_results; j++) {
            free(response->values[i]->results[j]);
        }
        free(response->values[i]->results);
        free(response->values[i]);
    }
    free(response->values);
    free(response);

    hash_table_free(ht);
}
