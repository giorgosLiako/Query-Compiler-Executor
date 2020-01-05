#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>
#include "DArray.h"

#define MAX_JOBS 4

typedef struct tuple {
    uint64_t key;
    uint64_t payload;
} tuple;

typedef struct relation {
    tuple *tuples;
    uint64_t num_tuples;
    uint64_t max_value;
    uint64_t min_value;
    size_t distinct_values;
} relation;

typedef struct metadata {
    uint64_t tuples;
    uint64_t columns;
    uint64_t max_values;
    uint64_t min_values;
    relation **data;
} metadata;

typedef struct histogram {
    int32_t array[256];
} histogram;

#endif