#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include "../src/join.h"
#include "../src/utilities.h"
#include "../src/parsing.h"
#include "../src/queries.h"
#include "../src/quicksort.h"
#include "../src/structs.h"

#define MAX_THREADS 4

static void free_query_list(DArray *query_list) {
   
   for (size_t i = 0 ; i < DArray_count(query_list); i++) {
        query *qr = (query *) DArray_get(query_list, i);
        if (qr != NULL){
            FREE(qr->relations);
            for(size_t j = 0 ; j < qr->predicates_size ; j++)
                FREE(qr->predicates[j].second);
            FREE(qr->predicates);
            FREE(qr->selects);
        }
    }
    DArray_destroy(query_list);
}

static void free_metadata_array(DArray *metadata_arr) {
   
    for (size_t i = 0 ; i < DArray_count(metadata_arr); i++) {
        metadata *met = (metadata *) DArray_get(metadata_arr, i);
        
        for(size_t j = 0 ; j < met->columns ; j++) {
            FREE(met->data[j]->tuples);
            FREE(met->data[j]);
        }
        FREE(met->data);
    } 
    DArray_destroy(metadata_arr);
}

int main(int argc, char *argv[]) {

    DArray *metadata_arr = DArray_create(sizeof(metadata), 10);

    check(read_relations(metadata_arr) != -1, "Something went wrong in reading the relations");
    
    DArray *query_list = NULL;
    
    thr_pool_t *pool = thr_pool_create(MAX_THREADS);
    while (1) {
        query_list = parser();

        if (DArray_count(query_list) == 0) {
            break;
        }

        execute_queries(query_list, metadata_arr, pool);
        
        free_query_list(query_list);
    }

    DArray_destroy(query_list);

    free_metadata_array(metadata_arr);
    
    thr_pool_destroy(pool);
    
    return EXIT_SUCCESS;

     error:
        return EXIT_FAILURE;
}
