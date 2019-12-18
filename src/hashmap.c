#include <stdint.h>
#include "hashmap.h"
#include "dbg.h"
#include "alloc_free.h"

Hashmap* Hashmap_create(size_t key_size, size_t data_size, Hashmap_compare compare, Hashmap_hash hash) {
    
    Hashmap *map = CALLOC(1, sizeof(Hashmap), Hashmap); 
    check_mem(map);

    map->compare = compare == NULL ? NULL : compare;
    map->key_size = key_size;
    map->data_size = data_size;
    map->hash = hash == NULL ? NULL : hash;
    map->buckets = DArray_create(sizeof(DArray), (size_t) BUCKETS);
    check_mem(map->buckets);
    map->buckets->end = map->buckets->capacity;
    

    return map;

    error:
        Hashmap_destroy(map);
        return NULL;
}

void Hashmap_destroy(Hashmap *map) {

    if (map) {
        for (ssize_t i = 0 ; i < DArray_count(map->buckets) ; i++) {
            DArray *bucket = (DArray *) DArray_get(map->buckets, i);
            if (bucket) {
                for (ssize_t j = 0 ; j < DArray_count(bucket) ; j++) {
                    HashMapNode *node = (HashMapNode *) DArray_get(bucket, j);
                    FREE(node->data);
                    FREE(node->key);
                }
                DArray_destroy(bucket);
            }
        }
        FREE(map->buckets->contents);
        FREE(map->buckets);
        FREE(map);
    }
}

static inline HashMapNode* Hashmap_node_create(Hashmap *map, uint32_t hash, void *key, void *data) {
    
    HashMapNode *node = CALLOC(1, sizeof(HashMapNode), HashMapNode);
    check_mem(node);

    node->key = CALLOC(1, map->key_size, void);
    memcpy(node->key, key, map->key_size);
    node->data = CALLOC(1, map->data_size, void);
    memcpy(node->data, data, map->data_size);
    node->hash = hash;

    return node;

    error:
        return NULL;
}

static inline DArray* Hashmap_find_bucket(Hashmap *map, void *key, int create, uint32_t *hash_out) {

    uint32_t hash = map->hash(key);
    int bucket_n = hash % BUCKETS;
    check(bucket_n >= 0, "Invalid bucket found: %d", bucket_n);
    *hash_out = hash;           //Return it to caller

    DArray *bucket = (DArray *) DArray_get(map->buckets, bucket_n);

    if (!bucket && create) {
        bucket = DArray_create(sizeof(HashMapNode), (size_t) BUCKETS);
        check_mem(bucket);
        DArray_set(map->buckets, bucket_n, bucket);
        FREE(bucket);
        bucket = (DArray *) DArray_get(map->buckets, bucket_n);
    }

    return bucket;

    error:
        return NULL;
}

int Hashmap_set(Hashmap *map, void *key, void *data) {
    
    uint32_t hash = 0;
    DArray *bucket = Hashmap_find_bucket(map, key, 1 , &hash);  
    check(bucket, "Error, can't create bucket");

    HashMapNode *node = Hashmap_node_create(map, hash, key, data);
    check_mem(node);

    DArray_push(bucket, node);
    FREE(node);

    return 0;

    error:
        return -1;
}

static inline int Hashmap_get_node(Hashmap *map, uint32_t hash, DArray *bucket, void *key) {

    for (uint32_t i = 0 ; i < DArray_count(bucket) ; i++) {
        HashMapNode *node = (HashMapNode *) DArray_get(bucket, i);
        check(node != NULL, "");
        if (node->hash == hash && !map->compare(node->key, key)) {
            return i;
        }
    }

    error:
        return -1;  
}

void* Hashmap_get(Hashmap *map, void *key) {

    uint32_t hash = 0;
    DArray *bucket = Hashmap_find_bucket(map, key, 0, &hash);
    if (!bucket) {
        return NULL;
    }

    int index = Hashmap_get_node(map, hash, bucket, key);
    if (index == -1) {
        return NULL;
    }

    HashMapNode *node = (HashMapNode *) DArray_get(bucket, index);
    if (!node) {
        return NULL;
    }

    return node->data;
}

void Hashmap_delete(Hashmap *map, void *key) {

    uint32_t hash = 0;
    int i;
    DArray *bucket = Hashmap_find_bucket(map, key, 0, &hash);
    check_mem(bucket);

    i = Hashmap_get_node(map, hash, bucket, key);
    check(i != -1, "Failed to get node");

    HashMapNode *node = (HashMapNode *) DArray_get(bucket, i);
    FREE(node->key);
    FREE(node->data);

    DArray_pop(bucket);

    error:
        return;
}
