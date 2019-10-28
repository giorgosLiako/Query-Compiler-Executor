#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>

typedef struct tuple {
    int64_t key;
    int64_t payload;
} tuple;

typedef struct relation {
    tuple *tuples;
    uint64_t num_tuples;
} relation;


typedef struct histogram {
    int32_t hist[256];
} histogram;

#endif