#ifndef PRED_ARRANGE_H
#define PRED_ARRANGE_H

#include <stdio.h>
#include <stdint.h>
#include "DArray.h"
#include "structs.h"
#include "alloc_free.h"
#include "dbg.h"
#include "utilities.h"
#include "queue.h"
#include "quicksort.h"

void arrange_predicates(query *qry);

#endif