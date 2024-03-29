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

typedef struct exists_info {
    ssize_t mid_result;
    ssize_t index;
} exists_info;

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *, histogram *, uint8_t, int, int);

void build_psum(histogram *, histogram *);

relation* allocate_reordered_array(relation *);

relation* build_reordered_array(relation *, relation *, histogram *, histogram *, uint8_t, int, int);

void free_reordered_array(relation *);

void swap_arrays(relation *, relation *);

int read_relations(DArray *);

exists_info relation_exists(DArray *, uint64_t , uint64_t);

ssize_t relation_exists_current(DArray *, uint64_t, uint64_t);

void execute_queries(DArray * , DArray * );

#endif