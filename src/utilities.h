#ifndef UTILITIES_H
#define UTILITIES_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "histogram.h"
#include "dbg.h"
#include "structs.h"
#include "DArray.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *, histogram *, uint8_t, int, int);

void build_psum(histogram *, histogram *);

relation* allocate_reordered_array(relation *);

relation* build_reordered_array(relation *, relation *, histogram *, histogram *, uint8_t, int, int);

void free_reordered_array(relation *);

void swap_arrays(relation *, relation *);

int read_relations(DArray *);

DArray* parser();

void execute_queries(DArray * , DArray * );

mid_result* create_entity(DArray *mid_results);

relation_info* add_to_entity(mid_result *res, uint32_t);

relation_info* relation_exists(DArray *, uint32_t );

#endif