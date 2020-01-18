#include "structs.h"
#include "queries.h"
#include "DArray.h"
#include "join.h"
#include "best_tree.h"
#include <stdarg.h>
#include "set.h"

predicate *dp_linear(int rel_size, predicate *predicates, int pred_size, const DArray *meta, int* null_join);