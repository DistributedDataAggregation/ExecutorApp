#include "hash_table_interface.h"
#include "hash_table.h"
#include "hash_table_optimized.h"

HashTableInterface* create_default_hash_table_interface()
{
    HashTableInterface* default_interface = malloc(sizeof(HashTableInterface));
    if (default_interface == NULL)
    {
        return;
    }

    default_interface->hash = hash;
    default_interface->create = hash_table_create;
    default_interface->free = hash_table_free;
    default_interface->insert = hash_table_insert;
    default_interface->search = hash_table_search;
    default_interface->remove = hash_table_delete;
    default_interface->print = hash_table_print;
    default_interface->combine_entries = hash_table_combine_entries;
    default_interface->update_value = hash_table_update_value;
    default_interface->combine_with_response = hash_table_combine_table_with_response;
    default_interface->optimized_combine = hash_table_combine_hash_tables;

    return default_interface;
}

HashTableInterface* create_optimized_hash_table_interface()
{
    HashTableInterface* optimized_interface = malloc(sizeof(HashTableInterface));
    if (optimized_interface == NULL)
    {
        return;
    }

    optimized_interface->hash = hash_farm;
    optimized_interface->create = hash_table_optimized_create;
    optimized_interface->free = hash_table_optimized_free;
    optimized_interface->insert = hash_table_optimized_insert;
    optimized_interface->search = hash_table_optimized_search;
    optimized_interface->remove = hash_table_optimized_delete;
    optimized_interface->print = hash_table_optimized_print;
    optimized_interface->combine_entries = hash_table_optimized_combine_entries;
    optimized_interface->update_value = hash_table_optimized_update_value;
    optimized_interface->combine_with_response = hash_table_combine_table_with_response_optimized;
    optimized_interface->optimized_combine = hash_table_optimized_combine_hash_tables;

    return optimized_interface;
}

void free_hash_table_interface(HashTableInterface* interface)
{
    if (interface == NULL)
    {
        return;
    }

    free(interface);
}
