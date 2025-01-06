#include <error.h>
#include <hash_table.h>

extern "C" {
#include "hash_table_optimized.h"
#include "hash_table_struct.h"
}

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>

TEST(OptimizedHashTableTest, CreateHashTable)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);
    EXPECT_EQ(ht->size, 16);
    EXPECT_EQ(ht->entries_count, 0);
    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, AddToHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_optimized_create(16, &error_info);

    const char* key = "war1|war2|war3|war4|war5";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
    ASSERT_NE(entry->values, nullptr);

    for (int i = 0; i < entry->n_values; i++)
    {
        entry->values[i].aggregate_function = MIN;
        entry->values[i].value = 100;
    }

    hash_table_optimized_insert(ht, entry, &error_info);
    HashTableEntry* returned = hash_table_optimized_search(ht, key);

    ASSERT_NE(returned, nullptr);
    EXPECT_EQ(returned->n_values, entry->n_values);
    EXPECT_STREQ(returned->key, key);

    for (int i = 0; i < entry->n_values; i++)
    {
        EXPECT_EQ(returned->values[i].aggregate_function, entry->values[i].aggregate_function);
        EXPECT_EQ(returned->values[i].value, entry->values[i].value);
    }

    returned->values[0].value += 100;

    HashTableEntry* returned2 = hash_table_optimized_search(ht, key);
    ASSERT_NE(returned2, nullptr);
    EXPECT_EQ(returned2->n_values, entry->n_values);
    EXPECT_EQ(returned2->values[0].value, 200);

    hash_table_optimized_delete(ht, key);
    HashTableEntry* returned3 = hash_table_optimized_search(ht, key);
    EXPECT_EQ(returned3, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, SearchInHashTable)
{
    HashTable* ht = hash_table_optimized_create(16, nullptr);

    const char* key = "key1";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
    entry->values[0].aggregate_function = MAX;
    entry->values[0].value = 200;

    hash_table_optimized_insert(ht, entry, nullptr);

    HashTableEntry* found = hash_table_optimized_search(ht, key);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ(found->key, key);
    EXPECT_EQ(found->n_values, 1);
    EXPECT_EQ(found->values[0].value, 200);

    HashTableEntry* not_found = hash_table_optimized_search(ht, "nonexistent_key");
    EXPECT_EQ(not_found, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, DeleteFromHashTable)
{
    HashTable* ht = hash_table_optimized_create(16, nullptr);

    const char* key = "key1";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
    entry->values[0].aggregate_function = AVG;
    entry->values[0].value = 300;

    hash_table_optimized_insert(ht, entry, nullptr);

    hash_table_optimized_delete(ht, key);
    HashTableEntry* found = hash_table_optimized_search(ht, key);
    EXPECT_EQ(found, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, CombineHashTables)
{
    ErrorInfo error_info = {0};

    HashTable* destination = hash_table_optimized_create(16, &error_info);
    HashTable* source = hash_table_optimized_create(16, &error_info);

    const char* key1 = "key1";
    HashTableEntry* entry1 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry1->key = strdup(key1);
    entry1->n_values = 1;
    entry1->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry1->n_values);
    entry1->values[0].aggregate_function = MIN;
    entry1->values[0].value = 300;
    entry1->values[0].type = HASH_TABLE_INT;

    const char* key2 = "key2";
    HashTableEntry* entry2 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry2->key = strdup(key2);
    entry2->n_values = 1;
    entry2->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry2->n_values);
    entry2->values[0].aggregate_function = MIN;
    entry2->values[0].value = 400;
    entry2->values[0].type = HASH_TABLE_INT;

    hash_table_optimized_insert(source, entry1, nullptr);
    hash_table_optimized_insert(source, entry2, nullptr);

    const char* key1_duplicate = "key1";
    HashTableEntry* entry1_duplicate = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry1_duplicate->key = strdup(key1_duplicate);
    entry1_duplicate->n_values = 1;
    entry1_duplicate->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry1_duplicate->n_values);
    entry1_duplicate->values[0].aggregate_function = MIN;
    entry1_duplicate->values[0].value = 500;
    entry1_duplicate->values[0].type = HASH_TABLE_INT;

    hash_table_optimized_insert(destination, entry1_duplicate, nullptr);

    hash_table_optimized_combine_hash_tables(destination, source, &error_info);

    HashTableEntry* result_entry1 = hash_table_optimized_search(destination, key1);
    ASSERT_NE(result_entry1, nullptr);
    EXPECT_EQ(result_entry1->values[0].value, 300);

    HashTableEntry* result_entry2 = hash_table_optimized_search(destination, key2);
    ASSERT_NE(result_entry2, nullptr);
    EXPECT_EQ(result_entry2->values[0].value, 400);

    HashTableEntry* result_source_entry2 = hash_table_optimized_search(source, key2);
    ASSERT_NE(result_source_entry2, nullptr);
    EXPECT_EQ(result_source_entry2 ->values[0].value, 400);

    HashTableEntry* result_source_entry1 = hash_table_optimized_search(destination, key1);
    ASSERT_NE(result_source_entry1, nullptr);
    EXPECT_EQ(result_source_entry1 ->values[0].value, 300);


    hash_table_optimized_free(destination);
    hash_table_optimized_free(source);
}

TEST(OptimizedHashTableTest, HandleCollision)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(2, &error_info);

    HashTableEntry* entry1 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry1->key = strdup("key1");
    entry1->n_values = 1;
    entry1->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry1->values[0].value = 100;
    entry1->values[0].aggregate_function = MIN;
    entry1->values[0].type= HASH_TABLE_INT;

    HashTableEntry* entry2 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry2->key = strdup("key2");
    entry2->n_values = 1;
    entry2->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry2->values[0].value = 200;
    entry2->values[0].aggregate_function = MAX;
    entry2->values[0].type= HASH_TABLE_INT;

    hash_table_optimized_insert(ht, entry1, nullptr);
    hash_table_optimized_insert(ht, entry2, nullptr);

    HashTableEntry* result1 = hash_table_optimized_search(ht, "key1");
    ASSERT_NE(result1, nullptr);
    EXPECT_EQ(result1->values[0].value, 100);

    HashTableEntry* result2 = hash_table_optimized_search(ht, "key2");
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->values[0].value, 200);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, ResizeHashTable)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(8, &error_info);

    for (int i = 0; i < 10; i++)
    {
        HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
        char key[10];
        sprintf(key, "key%d", i);
        entry->key = strdup(key);
        entry->n_values = 1;
        entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
        entry->values[0].value = i * 10;
        entry->values[0].aggregate_function = AVG;
        entry->values[0].type= HASH_TABLE_INT;

        hash_table_optimized_insert(ht, entry, &error_info);
    }

    for (int i = 0; i < 10; i++)
    {
        char key[10];
        sprintf(key, "key%d", i);
        HashTableEntry* result = hash_table_optimized_search(ht, key);
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->values[0].value, i * 10);
    }

    hash_table_optimized_free(ht);
}


TEST(OptimizedHashTableTest, HandleNullInsert)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    hash_table_optimized_insert(ht, nullptr, &error_info);
    EXPECT_EQ(error_info.error_code, -1);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleInsertDuplicateKey)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);

    HashTableEntry* entry1 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry1->key = strdup("key1");
    entry1->n_values = 1;
    entry1->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry1->values[0].value = 100;
    entry1->values[0].aggregate_function = MIN;

    HashTableEntry* entry2 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    entry2->key = strdup("key1");
    entry2->n_values = 1;
    entry2->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry2->values[0].value = 200;
    entry2->values[0].aggregate_function = MIN;

    hash_table_optimized_insert(ht, entry1, &error_info);
    hash_table_optimized_insert(ht, entry2, &error_info);

    HashTableEntry* result = hash_table_optimized_search(ht, "key1");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values[0].value, 100); // Should retain the original value.

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleResize)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(4, &error_info);
    ASSERT_NE(ht, nullptr);

    for (int i = 0; i < 10; i++)
    {
        HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
        char key[10];
        sprintf(key, "key%d", i);
        entry->key = strdup(key);
        entry->n_values = 1;
        entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
        entry->values[0].value = i;
        entry->values[0].aggregate_function = AVG;

        hash_table_optimized_insert(ht, entry, nullptr);
    }

    EXPECT_GE(ht->size, 16); // Ensure table resized correctly.

    for (int i = 0; i < 10; i++)
    {
        char key[10];
        sprintf(key, "key%d", i);
        HashTableEntry* result = hash_table_optimized_search(ht, key);
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->values[0].value, i);
    }

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleDeleteNonExistentKey)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    hash_table_optimized_delete(ht, "nonexistent_key");

    HashTableEntry* result = hash_table_optimized_search(ht, "nonexistent_key");
    EXPECT_EQ(result, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, StressTestLargeInserts)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    const int num_entries = 100000;
    for (int i = 0; i < num_entries; i++)
    {
        HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
        char key[20];
        sprintf(key, "key%d", i);
        entry->key = strdup(key);
        entry->n_values = 1;
        entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
        entry->values[0].value = i;
        entry->values[0].aggregate_function = AVG;

        hash_table_optimized_insert(ht, entry, &error_info);
    }

    for (int i = 0; i < num_entries; i++)
    {
        char key[20];
        sprintf(key, "key%d", i);
        HashTableEntry* result = hash_table_optimized_search(ht, key);
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->values[0].value, i);
    }

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleInsertNullKey)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = nullptr; // NULL key
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    ASSERT_NE(entry->values, nullptr);

    entry->values[0].value = 100;
    entry->values[0].aggregate_function = AVG;

    hash_table_optimized_insert(ht, entry, &error_info);
    EXPECT_EQ(error_info.error_code, INTERNAL_ERROR);

    free(entry->values);
    free(entry);
    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, SearchDeletedKey)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    const char* key = "key1";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry->values[0].value = 100;
    entry->values[0].aggregate_function = AVG;

    hash_table_optimized_insert(ht, entry, nullptr);

    hash_table_optimized_delete(ht, key);

    HashTableEntry* result = hash_table_optimized_search(ht, key);
    EXPECT_EQ(result, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleEmptyTableSearch)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    const char* key = "nonexistent_key";
    HashTableEntry* result = hash_table_optimized_search(ht, key);
    EXPECT_EQ(result, nullptr);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, HandleExtremeValues)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(16, &error_info);
    ASSERT_NE(ht, nullptr);

    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    ASSERT_NE(entry, nullptr);

    entry->key = strdup("extreme_key");
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
    entry->values[0].value = LONG_MAX; // Extreme value
    entry->values[0].aggregate_function = MAX;

    hash_table_optimized_insert(ht, entry, nullptr);

    HashTableEntry* result = hash_table_optimized_search(ht, "extreme_key");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->values[0].value, LONG_MAX);

    hash_table_optimized_free(ht);
}

TEST(OptimizedHashTableTest, CombineEmptyTables)
{
    ErrorInfo error_info = {0};
    HashTable* ht1 = hash_table_optimized_create(16, &error_info);
    HashTable* ht2 = hash_table_optimized_create(16, &error_info);

    ASSERT_NE(ht1, nullptr);
    ASSERT_NE(ht2, nullptr);

    hash_table_optimized_combine_hash_tables(ht1, ht2, &error_info);

    EXPECT_EQ(ht1->entries_count, 0);

    hash_table_optimized_free(ht1);
    hash_table_optimized_free(ht2);
}

TEST(OptimizedHashTableTest, HandleResizeAfterDeletions)
{
    ErrorInfo error_info = {0};
    HashTable* ht = hash_table_optimized_create(8, &error_info);
    ASSERT_NE(ht, nullptr);

    for (int i = 0; i < 10; i++)
    {
        HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
        char key[10];
        sprintf(key, "key%d", i);
        entry->key = strdup(key);
        entry->n_values = 1;
        entry->values = (HashTableValue*)malloc(sizeof(HashTableValue));
        entry->values[0].value = i;
        entry->values[0].aggregate_function = AVG;

        hash_table_optimized_insert(ht, entry, nullptr);
    }

    // Delete some entries
    for (int i = 0; i < 5; i++)
    {
        char key[10];
        sprintf(key, "key%d", i);
        hash_table_optimized_delete(ht, key);
    }

    EXPECT_GE(ht->size, 16);

    hash_table_optimized_free(ht);
}
