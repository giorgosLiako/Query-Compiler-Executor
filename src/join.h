#ifndef JOIN_H
#define JOIN_H

#include <stdio.h>
#include <stdint.h>
#include <stdarg.h>
#include "DArray.h"
#include "structs.h"
#include "alloc_free.h"
#include "dbg.h"
#include "utilities.h"
#include "quicksort.h"
#include "queries.h"
#include "avl_tree.h"

typedef struct join_result {
    DArray *no_duplicates[2];
    DArray *results[2];
} join_result;

typedef struct join_info {
    uint32_t relR;
    uint32_t relS;
    uint32_t colR;
    uint32_t colS;
    uint32_t predR_id;
    uint32_t predS_id;
    int join_id;
    join_result join_res;
} join_info;

typedef struct join_arguments {
    uint32_t start;
    uint32_t end;
    uint16_t **indices_to_check;
    DArray *rel_R;
    DArray *rel_S;
    DArray *queue_R;
    DArray *queue_S;
    join_result join_res;
} join_arguments;

typedef struct rel_info {
    DArray *rel;
    DArray *queue;
    uint32_t jobs_to_create;
} rel_info; 

typedef struct payloads {
    uint64_t payload_R;
    uint64_t payload_S;
} payloads;

join_result scan_join(DArray *relR, DArray *relS);

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, thr_pool_t *pool);

#endif