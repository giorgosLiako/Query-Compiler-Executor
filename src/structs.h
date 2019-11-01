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
    relation *relR;
    relation *reorderedR;
    uint8_t current_byte;
    uint32_t size;
    uint32_t start;
    histogram *hist;
    histogram *psum;
} stack_node;

typedef struct queue_node {
    histogram *hist;
    histogram *psum;
} queue_node;

#endif