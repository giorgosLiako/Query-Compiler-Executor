#ifndef FILTER_H
#define FILTER_H

#include "queries.h"
#include "DArray.h"
#include "utilities.h"
#include "queries.h"

int execute_filter(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array);

#endif