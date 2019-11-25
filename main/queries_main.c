#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <CUnit/Basic.h>
#include <CUnit/CUnit.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"
#include "../src/utilities.h"


int main() {

    metadata_array *arr = MALLOC(metadata_array, 1);
    check(read_relations(arr) != -1, "Something went wrong in reading the relations");

    FREE(arr);
    return EXIT_SUCCESS;

    error:
        FREE(arr);
        return EXIT_FAILURE;
}
