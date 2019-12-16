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
    relation_column* selects;
    size_t select_size;
} query;

typedef struct relation_info {
    uint32_t relation;
    int32_t last_col_sorted;
    DArray *payloads;
} relation_info;

typedef struct mid_result {
    DArray *rel_infos;
} mid_result;


results = malloc(sizeof(uint64_t) * DArray_count(rel_infos) + 1);


   (rowid0, rowid1)
   0.1 = 1.0 & 1.0 = 2.0

   (rowid0, rowid1) uint64_t *valuesOf1.0 , relation(2.0)

   =>

   (rowid0, rowid1, rowid2)

////////////////////////////////////////////////////////////////

    0.1 = 1.0 & 2.0 = 1.0

    (rowid0, rowid1) uint64_t *valuesOf1.0, relation(2.0)

    =>

    (rowid0, rowid1, rowid2)

//////////////////////////////////////////////////////////////////

    (rowid0, rowid1)
    0.1 = 1.0 & 1.1 = 2.0

    (rowid0, rowid1) uint64_t *valuesOf1.1, relation(2.0)

    =>

    (rowid0, rowid1, rowid2);


////////////////////////////////////////////////////////////////////

    (rowid0, rowid1)
    0.1 = 1.0 & 1.2 = 0.2

    relation(1.2), relation(0.2)

    =>

    DArray(1.2), DArray(0.2)

////////////////////////////////////////////////////////////////////

    (rowid0, rowid1)
    0.1 = 1.0 & 2.2 = 3.2 & 1.4 = 0.2

    new entity => 2rel_info => add => (rowid2, rowid3) => relation(2.2), relation(3.2)
    
    =>

    DArray(2.2), DArray(3.2)

#endif