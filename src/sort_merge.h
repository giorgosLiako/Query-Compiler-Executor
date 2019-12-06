#ifndef SORT_MERGE_H
#define SORT_MERGE_H

#include "structs.h"

void join_relations(relation *relR, relation *relS);

void self_join(relation *relR, relation *relS);

int iterative_sort(relation *rel);

#endif