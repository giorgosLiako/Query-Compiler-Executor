#ifndef STRUCTS_H
#define STRUCTS_H

#include <stdint.h>
#include "DArray.h"

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

typedef struct relation_column {
    int relation;
    int column;
} relation_column;

typedef struct predicate{
    int type;
    relation_column first;
    void* second;
    char operator;
} predicate;

typedef struct query {
    int* relations;
    int relations_size;
    predicate* predicates;
    int predicates_size;
    relation_column* select;
    int select_size;
} query;

typedef struct mid_result {
    int relation;
    DArray* payloads;
} mid_result;

#endif