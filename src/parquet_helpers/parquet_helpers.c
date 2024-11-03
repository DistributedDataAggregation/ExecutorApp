//
// Created by karol on 03.11.24.
//
#include "parquet_helpers.h"

void report_g_error(GError* error) {
    if (error != NULL) {
        g_print("Error occurred: %s\n", error->message);
        g_error_free(error);  // Free the GError after use
    } else {
        g_print("Unknown error occurred.\n");
    }
}
