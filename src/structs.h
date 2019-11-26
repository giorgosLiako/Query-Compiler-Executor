#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

typedef struct tuple {
    uint64_t key;
    uint64_t payload;
} tuple;

typedef struct relation {
    tuple *tuples;
    uint64_t num_tuples;
} relation;

typedef struct metadata {
    uint64_t tuples;
    uint64_t columns;
    relation **data;
} metadata;

#endif