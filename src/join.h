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

#define CLASSIC_JOIN 1
#define JOIN_SORT_LHS 2
#define JOIN_SORT_RHS 3
#define SCAN_JOIN 4
#define DO_NOTHING 5

typedef struct result {
    DArray *results[2];
    DArray *non_duplicates[2];
} join_result;

typedef struct join_arguments {
    uint32_t start;
    uint32_t end;
    uint16_t **indices_to_check;
    relation *rel_R;
    relation *rel_S;
    DArray *queue_R;
    DArray *queue_S;
    uint32_t found;
} join_arguments;

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, thr_pool_t *pool, DArray *queue_S, DArray *queue_R);

#endif