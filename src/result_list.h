#ifndef RESULT_LIST_H
#define RESULT_LIST_H

#include <stdint.h>

typedef struct list_node{
    int64_t row_id_1;
    int64_t row_id_2;
} list_node;

typedef struct result {
    list_node buffer[65536];
    struct result *next;
} result;

#endif