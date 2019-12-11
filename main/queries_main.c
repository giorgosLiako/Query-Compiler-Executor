#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/structs.h"
#include "../src/dbg.h"
#include "../src/utilities.h"
#include "../src/DArray.h"
#include "../src/parsing.h"


int main() {

    DArray *metadata_arr = DArray_create(sizeof(metadata), 10);

    check(read_relations(metadata_arr) != -1, "Something went wrong in reading the relations");

    DArray *query_list = parser(metadata_arr);
    check(query_list != NULL, "Parsing failed");
    
    execute_queries(query_list, metadata_arr);

    DArray_destroy(query_list);
    DArray_destroy(metadata_arr);
   
    return EXIT_SUCCESS;

     error:
         DArray_destroy(metadata_arr);
         return EXIT_FAILURE;
}
