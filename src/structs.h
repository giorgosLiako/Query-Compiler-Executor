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


typedef struct histogram {
    int32_t array[256];
} histogram;


typedef struct stack_node {
    histogram *hist;
    histogram *psum;
} stack_node;

#endif