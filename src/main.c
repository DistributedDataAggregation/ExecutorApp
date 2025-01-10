#include <stdio.h>
#include <stdlib.h>
#include "main_thread/main_thread.h"

int main(void)
{
    int main_thread_result = main_thread_run();

    if (main_thread_result == EXIT_FAILURE)
    {
        fprintf(stderr, "Main thread encountered error in it's execution\n");
    }

    return main_thread_result;
}
