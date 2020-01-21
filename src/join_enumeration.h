#include "structs.h"
#include "queries.h"
#include "join.h"
#include "best_tree.h"
#include <stdarg.h>
#include "set.h"

predicate *dp_linear(uint32_t *relations, int rel_size, predicate *predicates, int pred_size, const metadata *meta, int* null_join, ssize_t index);