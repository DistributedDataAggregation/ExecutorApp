//
// Created by karol on 03.11.24.
//

#ifndef PARQUET_HELPERS_H
#define PARQUET_HELPERS_H

#include <parquet-glib/arrow-file-reader.h>
#include "error_handling.h"

void report_g_error(GError* error, ErrorInfo* err, const char* error_message);

#endif //PARQUET_HELPERS_H
