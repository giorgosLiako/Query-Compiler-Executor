#ifndef JOIN_UTILITIES_H
#define JOIN_UTILITIES_H

#define CLASSIC_JOIN 1
#define JOIN_SORT_LHS 2
#define JOIN_SORT_RHS 3
#define SCAN_JOIN 4
#define DO_NOTHING 5

#include "join.h"
#include "DArray.h"
#include "structs.h"
#include "queries.h"

void update_mid_results(DArray *mid_results_array, DArray *metadata_arr, join_info info, rel_info *rel[2]);

int sort_relations(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, rel_info *rel[2], thr_pool_t *pool);

#endif
