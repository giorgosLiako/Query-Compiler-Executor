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

typedef struct metadata_array {
    uint64_t tuples;
    uint64_t columns;
    relation **data;
} metadata_array;

typedef struct relation_column {
    int relation;
    int column;
} relation_column;

typedef struct predicate{
    int type;
    relation_column* first;
    void* second;
    char operator;
} predicate;

typedef struct query {
    int* relations;

} query;

#endif