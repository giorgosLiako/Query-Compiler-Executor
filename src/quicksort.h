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
    DArray *relations[2];
    DArray *queue;
    uint32_t start;
    uint32_t end;
    int index;
} sort_args;

int iterative_sort(DArray *tuples, DArray **retval, uint32_t *jobs_created, thr_pool_t *pool);

void random_quicksort(DArray *tuples, ssize_t low, ssize_t high);

#endif