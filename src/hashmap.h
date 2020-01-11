#ifndef HASHMAP_H
#define HASHMAP_H

#include <stdint.h>
#include "DArray.h"

#define BUCKETS 1000

typedef int32_t (*Hashmap_compare) (const void *, const void *);

typedef uint32_t (*Hashmap_hash) (void *key);

typedef struct Hashmap {
    DArray *buckets;
    Hashmap_compare compare;
    Hashmap_hash hash;
    size_t key_size;
    size_t data_size;
} Hashmap;


typedef struct {
    void *key;
    void *data;
    uint32_t hash;
} HashMapNode;


Hashmap *Hashmap_create(size_t key_size, size_t data_size, Hashmap_compare compare, Hashmap_hash hash);

void Hashmap_destroy(Hashmap *map);

int32_t Hashmap_set(Hashmap *map, void *key, void *data);

void* Hashmap_get(Hashmap *map, void *key);

void Hashmap_delete(Hashmap *map, void *key);

#endif
