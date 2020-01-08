#ifndef UTILITIES_H
#define UTILITIES_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <math.h>
#include "alloc_free.h"
#include "dbg.h"
#include "DArray.h"
#include "structs.h"
#include "queries.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *, histogram *, uint8_t, int, int);

void build_psum(histogram *, histogram *);

relation* allocate_reordered_array(relation *);

relation* build_reordered_array(relation *, relation *, histogram *, histogram *, uint8_t, int, int);

void free_reordered_array(relation *);

void swap_arrays(relation *, relation *);

int read_relations(DArray *);

exists_info relation_exists(DArray *mid_results_array, uint64_t relation, uint64_t predicate_id);

ssize_t relation_exists_current(DArray *mid_results, uint64_t relation, uint64_t predicate_id);

void build_histogram_darray(DArray *tuples, histogram *hist, uint8_t wanted_byte, int start, int size);

DArray* build_reordered_darray(DArray *reorder_rel, DArray *prev_rel, histogram *histo, histogram *psum, uint8_t wanted_byte, int start, int size);

DArray* allocate_reordered_darray(DArray *rel);

void swap_darrays(DArray *r1, DArray *r2);

#endif