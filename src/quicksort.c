#include <stdio.h>
#include <stdint.h>
#include "quicksort.h"

ssize_t random_in_range(ssize_t low, ssize_t high) {

    ssize_t r = rand();

    r = (r << 31) | rand();

    return r % (high - low + 1) + low;
}

void swap(tuple *tuples, ssize_t i, ssize_t j) {
    tuple tup = tuples[i];
    tuples[i] = tuples[j];
    tuples[j] = tup;
}


ssize_t hoare_partition(tuple *tuples, ssize_t low, ssize_t high) {

    ssize_t i = low - 1;
    ssize_t j = high + 1;

    ssize_t random = random_in_range(low, high);

    swap(tuples, low, random);

    uint64_t pivot = tuples[high].key;

    while (1) {

        do {
            i++;
        } while (tuples[i].key < pivot);

        do {
            j--;
        } while (tuples[j].key > pivot);

        if (i >= j) {
            return j;
        }

        swap(tuples, i, j);
    }
}

void random_quicksort(tuple *tuples, ssize_t low, ssize_t high) {

    if (low >= high) {
        return;
    }

    ssize_t pivot = hoare_partition(tuples, low, high);

    random_quicksort(tuples, low, pivot);
    random_quicksort(tuples, pivot + 1, high);
}
