#include <stdio.h>
#include <stdlib.h>

#include "main_thread/main_thread.h"
#include "temporary_testing/test.h"

int main(void)
{
    run_all_tests();
    int main_thread_result = run_main_thread();

    if(main_thread_result == EXIT_FAILURE) {
        fprintf(stderr, "Main thread encountered error in it's execution\n");
    }

    return main_thread_result;
}
