#ifndef QUICKSORT_H
#define QUICKSORT_H

#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <semaphore.h>
#include <unistd.h>
#include "DArray.h"
#include "structs.h"
#include "utilities.h"

typedef struct queue_node {
    ssize_t base;
    ssize_t size;
    uint16_t byte;
} queue_node;

typedef struct sort_args {
    relation *relations[2];
    DArray *queue;
    uint32_t start;
    uint32_t end;
    int index;
} sort_args;

int iterative_sort(relation *rel, DArray **, thr_pool_t *pool);

#endif