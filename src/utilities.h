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

void print_select(relation_column *, size_t);

void print_predicates(predicate*, size_t);

void print_relations(int*, size_t);

void execute_query(query* , DArray* );

#endif