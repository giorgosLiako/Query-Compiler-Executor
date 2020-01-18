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
#include "../src/thr_pool.h"
#include "../src/stretchy_buffer.h"

#define MAX_THREADS 4

static void free_query_list(query *query_list) {
   
   for (size_t i = 0 ; i < buf_len(query_list); i++) {
        query *qr = &(query_list[i]);
        if (qr != NULL){
            FREE(qr->relations);
            for(size_t j = 0 ; j < qr->predicates_size ; j++)
                FREE(qr->predicates[j].second);
            FREE(qr->predicates);
            FREE(qr->selects);
        }
    }
    buf_free(query_list);
}

static void free_metadata_array(metadata *metadata_arr) {
   
    for (size_t i = 0 ; i < buf_len(metadata_arr) ; i++) {
        metadata *met = &(metadata_arr[i]);
        
        for(size_t j = 0 ; j < met->columns ; j++) {
            FREE(met->data[j]->tuples);
            FREE(met->data[j]->stats->array);
            FREE(met->data[j]->stats);
            FREE(met->data[j]);
        }
        FREE(met->data);
    }
    buf_free(metadata_arr);
}

int main(int argc, char *argv[]) {

    metadata *metadata_arr = read_relations();
    check_mem(metadata_arr);

    thr_pool_t *pool = thr_pool_create(4);

    while (1) {
        query *query_list = parser();

        if (buf_len(query_list) == 0) {
            break;
        }
        execute_queries(query_list, metadata_arr, pool);
        
        free_query_list(query_list);
    }

    thr_pool_destroy(pool);

    free_metadata_array(metadata_arr);
    
        
    return EXIT_SUCCESS;

     error:
        return EXIT_FAILURE;
}