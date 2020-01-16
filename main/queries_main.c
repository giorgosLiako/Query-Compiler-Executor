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

#define MAX_THREADS 8

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
            DArray_destroy(met->data[j]->tuples);
            FREE(met->data[j]->stats->array);
            FREE(met->data[j]->stats);
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

    thr_pool_t *pool = thr_pool_create(4);

    while (1) {
        query_list = parser();

        if (DArray_count(query_list) == 0) {
            break;  
        }
        
        execute_queries(query_list, metadata_arr, pool);
        
        free_query_list(query_list);
    }

    thr_pool_destroy(pool);

    DArray_destroy(query_list); 

    free_metadata_array(metadata_arr);
    
        
    return EXIT_SUCCESS;

     error:
        return EXIT_FAILURE;
}

/*
    FILE *fp = fopen(argv[1], "r");

    ssize_t nread;
    char *linptr = NULL;
    size_t n = 0;
    uint64_t num_tuples_R = 0;
    while ( (nread = getline(&linptr, &n, fp)) != -1) {
        num_tuples_R++;
    }
    sleep(2);
    tuple *tuples = MALLOC(tuple, num_tuples_R);

    linptr = NULL;
    n = 0;
    rewind(fp);
    size_t i = 0;
    while ( (nread = getline(&linptr, &n, fp)) != -1) {
        sscanf(linptr, "%lu,%lu\n", &tuples[i].key, &tuples[i].payload);
        i++;
    }

    DArray *tuples_R = DArray_create(sizeof(tuple), num_tuples_R);
    for (ssize_t i = 0 ; i < num_tuples_R ; i++) {
        tuple to_push = tuples[i];
        DArray_push(tuples_R, &to_push);
    }

    fp = fopen(argv[2], "r");

    linptr = NULL;
    n = 0;
    uint64_t num_tuples_S = 0;
    while ( (nread = getline(&linptr, &n, fp)) != -1) {
        num_tuples_S++;
    }
    tuple *tuples_S_tmp = MALLOC(tuple, num_tuples_S);

    linptr = NULL;
    n = 0;
    rewind(fp);
    i = 0;
    while ( (nread = getline(&linptr, &n, fp)) != -1) {
        sscanf(linptr, "%lu,%lu\n", &tuples_S_tmp[i].key, &tuples_S_tmp[i].payload);
        i++;
    }

    DArray *tuples_S = DArray_create(sizeof(tuple), num_tuples_S);
    for (ssize_t i = 0 ; i < num_tuples_S ; i++) {
        DArray_push(tuples_S, &tuples_S_tmp[i]);
    }
    FREE(tuples);
    FREE(tuples_S_tmp);

    DArray *queue_R, *queue_S;
    uint32_t jobs_created_R, jobs_created_S;

    iterative_sort(tuples_R, &queue_R, &jobs_created_R, pool);
    iterative_sort(tuples_S, &queue_S, &jobs_created_S, pool);

    uint32_t jobs_to_create = jobs_created_R < jobs_created_S ? jobs_created_R : jobs_created_S;
    join_relations(tuples_R, tuples_S, queue_R, queue_S, jobs_to_create, pool);*/
