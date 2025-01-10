extern "C" {
#include "hash_table.h"
}

#include <gtest/gtest.h>
#include <cstring>
#include <cstdlib>


TEST(HashTableTest, CreateHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_create(10, 100, &error_info);
    ASSERT_NE(ht, nullptr);
    EXPECT_EQ(ht->size, 10);

    hash_table_free(ht);
}

TEST(HashTableTest, AddToHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_create(10, 100, &error_info);

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

    hash_table_insert(ht, entry, &error_info);
    HashTableEntry* returned = hash_table_search(ht, key);

    ASSERT_NE(returned, nullptr);
    EXPECT_EQ(returned->n_values, entry->n_values);
    EXPECT_STREQ(returned->key, key);

    for (int i = 0; i < entry->n_values; i++)
    {
        EXPECT_EQ(returned->values[i].aggregate_function, entry->values[i].aggregate_function);
        EXPECT_EQ(returned->values[i].value, entry->values[i].value);
    }

    returned->values[0].value += 100;

    HashTableEntry* returned2 = hash_table_search(ht, key);
    ASSERT_NE(returned2, nullptr);
    EXPECT_EQ(returned2->n_values, entry->n_values);
    EXPECT_EQ(returned2->values[0].value, 200);

    hash_table_delete(ht, key);
    HashTableEntry* returned3 = hash_table_search(ht, key);
    EXPECT_EQ(returned3, nullptr);

    hash_table_free(ht);
}

TEST(HashTableTest, SearchInHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_create(10, 100, &error_info);

    const char* key = "key1";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry == nullptr)
    {
        hash_table_free(ht);
        return;
    }
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    if (entry->key == nullptr)
    {
        free(entry);
        hash_table_free(ht);
        return;
    }
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
    if (entry->values == nullptr)
    {
        hash_table_free(ht);
        free(entry);
        return;
    }
    entry->values[0].aggregate_function = MAX;
    entry->values[0].value = 200;

    hash_table_insert(ht, entry, &error_info);
    if (error_info.error_code != 0)
    {
        free(entry->values);
        free(entry->key);
        free(entry);
        hash_table_free(ht);
        return;
    }

    HashTableEntry* found = hash_table_search(ht, key);
    ASSERT_NE(found, nullptr);
    EXPECT_STREQ(found->key, key);
    EXPECT_EQ(found->n_values, 1);
    EXPECT_EQ(found->values[0].value, 200);

    HashTableEntry* not_found = hash_table_search(ht, "nonexistent_key");
    EXPECT_EQ(not_found, nullptr);

    hash_table_free(ht);
}

TEST(HashTableTest, DeleteFromHashTable)
{
    ErrorInfo error_info = {0};

    HashTable* ht = hash_table_create(10, 100, &error_info);

    const char* key = "key1";
    HashTableEntry* entry = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry== nullptr)
    {
        hash_table_free(ht);
        return;
    }
    ASSERT_NE(entry, nullptr);

    entry->key = strdup(key);
    entry->n_values = 1;
    entry->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry->n_values);
    if (entry->values == nullptr)
    {
        free(entry->values);
        free(entry);
        return;
    }
    entry->values[0].aggregate_function = AVG;
    entry->values[0].value = 300;

    hash_table_insert(ht, entry, &error_info);

    hash_table_delete(ht, key);
    HashTableEntry* found = hash_table_search(ht, key);
    EXPECT_EQ(found, nullptr);

    hash_table_free(ht);
}

TEST(HashTableTest, CombineEntries)
{
    ErrorInfo error_info = {0};

    HashTableEntry entry1;
    entry1.key = strdup("key1");
    entry1.n_values = 1;
    entry1.values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry1.n_values);
    entry1.values[0].aggregate_function = MIN;
    entry1.values[0].value = 500;
    entry1.values[0].type = HASH_TABLE_INT;

    HashTableEntry entry2;
    entry2.key = strdup("key1");
    entry2.n_values = 1;
    entry2.values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry2.n_values);
    entry2.values[0].aggregate_function = MIN;
    entry2.values[0].value = 300;
    entry2.values[0].type = HASH_TABLE_INT;

    hash_table_combine_entries(&entry1, &entry2, &error_info);
    EXPECT_EQ(entry1.values[0].value, 300);

    free(entry1.values);
    free(entry2.values);
}

TEST(HashTableTest, CombineHashTables)
{
    ErrorInfo error_info = {0};

    HashTable* destination = hash_table_create(10, 100, &error_info);
    HashTable* source = hash_table_create(10, 100, &error_info);

    const char* key1 = "key1";
    HashTableEntry* entry1 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry1 == nullptr)
    {
        return;
    }
    entry1->key = strdup(key1);
    entry1->n_values = 1;
    entry1->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry1->n_values);
    if (entry1->values == nullptr)
    {
        free(entry1->values);
        free(entry1);
        return;
    }
    entry1->values[0].aggregate_function = MIN;
    entry1->values[0].value = 300;

    const char* key2 = "key2";
    HashTableEntry* entry2 = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry2 == nullptr)
    {
        free(entry2);
        return;
    }
    entry2->key = strdup(key2);
    entry2->n_values = 1;
    entry2->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry2->n_values);
    if (entry2->values == nullptr)
    {
        free(entry2->values);
        free(entry2);
        return;
    }
    entry2->values[0].aggregate_function = MIN;
    entry2->values[0].value = 400;

    hash_table_insert(source, entry1, &error_info);
    hash_table_insert(source, entry2, &error_info);

    const char* key1_duplicate = "key1";
    HashTableEntry* entry1_duplicate = (HashTableEntry*)malloc(sizeof(HashTableEntry));
    if (entry1_duplicate == nullptr)
    {
        free(entry1_duplicate);
        return;
    }
    entry1_duplicate->key = strdup(key1_duplicate);
    entry1_duplicate->n_values = 1;
    entry1_duplicate->values = (HashTableValue*)malloc(sizeof(HashTableValue) * entry1_duplicate->n_values);
    if (entry1_duplicate->values == nullptr)
    {
        free(entry1_duplicate->values);
        free(entry1_duplicate);
        return;
    }
    entry1_duplicate->values[0].aggregate_function = MIN;
    entry1_duplicate->values[0].value = 500;

    hash_table_insert(destination, entry1_duplicate, &error_info);

    hash_table_free(destination);
    hash_table_free(source);
}
