//
// Created by karol on 03.11.24.
//
#include "parquet_helpers.h"

void report_g_error(GError* error, ErrorInfo* err, const char* error_message) {

    if (err == NULL) {
        LOG_INTERNAL_ERR("Passed error info was NULL");
        return;
    }

    if (error != NULL) {
        g_print("%s: %s\n", error_message, error->message);
        SET_ERR(err, G_ERROR, error_message, error->message);
        g_error_free(error);  // Free the GError after use
    } else {
        g_print("%s: Unknown error occurred.\n", error_message);
        SET_ERR(err, INTERNAL_ERROR, error_message, "Unknown error occurred");
    }
}
