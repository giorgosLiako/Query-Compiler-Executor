#ifndef FILTER_H
#define FILTER_H

#include <stdio.h>
#include <stdint.h>
#include "DArray.h"
#include "structs.h"
#include "alloc_free.h"
#include "dbg.h"
#include "utilities.h"
#include "queue.h"
#include "quicksort.h"

int execute_filter(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results);

#endif