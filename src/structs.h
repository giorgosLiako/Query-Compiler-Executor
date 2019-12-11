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
    uint64_t relation;
    uint64_t column;
} relation_column;

typedef struct predicate{
    int8_t type;
    relation_column first;
    void *second;
    char operator;
} predicate;

typedef struct query {
    uint32_t* relations;
    size_t relations_size;
    predicate* predicates;
    size_t predicates_size;
    relation_column* select;
    size_t select_size;
} query;

typedef struct mid_result {
    uint32_t relation;
    int32_t last_column_sorted;
    DArray* payloads;
} mid_result;

#endif