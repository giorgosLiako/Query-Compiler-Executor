#ifndef JOIN_H
#define JOIN_H

#include <stdio.h>
#include <stdint.h>
#include "DArray.h"
#include "structs.h"
#include "alloc_free.h"
#include "dbg.h"
#include "utilities.h"
#include "queue.h"
#include "quicksort.h"

#define CLASSIC_JOIN 1
#define JOIN_SORT_LHS 2
#define JOIN_SORT_RHS 3
#define SCAN_JOIN 4
#define DO_NOTHING 5

typedef struct result {
    DArray *results[2];
} join_result;


int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results);

#endif