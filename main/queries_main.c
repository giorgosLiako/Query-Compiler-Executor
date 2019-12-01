#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"
#include "../src/utilities.h"
#include "../src/DArray.h"


int main() {

    DArray *metadata_arr = DArray_create(sizeof(metadata), 10);

    check(read_relations(metadata_arr) != -1, "Something went wrong in reading the relations");

    DArray *query_list = parser();
    for (size_t i = 0; i < DArray_count(query_list); i++) {

        query *tmp_data = (query*) DArray_get(query_list, i);

        print_relations(tmp_data->relations, tmp_data->relations_size);
        print_predicates(tmp_data->predicates, tmp_data->predicates_size);
        print_select(tmp_data->select, tmp_data->select_size);
    }
    
    for (size_t i = 0; i < DArray_count(query_list); i++) {

        query *tmp_data = (query*) DArray_get(query_list, i);

        execute_query(tmp_data , metadata_arr);
    }


    DArray_destroy(query_list);

    DArray_destroy(metadata_arr);
    return EXIT_SUCCESS;

     error:
         FREE(metadata_arr);
         return EXIT_FAILURE;
}
