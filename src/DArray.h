#ifndef _DArray_h
#define _DArray_h
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include "dbg.h"
#include "alloc_free.h"

typedef void (*DArray_printf) (FILE *, void *);

typedef int (*DArray_compare) (const void *, const void *);

typedef struct DArray {
    int32_t end;
    int32_t capacity;
    uint32_t count;
    size_t element_size;
    void **contents;
} DArray;

DArray* DArray_create(size_t element_size, int32_t initial_capacity);

void DArray_destroy(DArray *array);

void DArray_clear(DArray *array);

int DArray_expand(DArray *array);

int DArray_push(DArray *array, void *element);

int DArray_pop(DArray *array);

int DArray_resize(DArray *array, int32_t newsize);

void DArray_clear_destroy(DArray *array);

void DArray_sort(DArray *array, DArray_compare);

void DArray_print(FILE *out, DArray *array, DArray_printf print_func);

int DArray_binary_search(DArray *array, DArray_compare compare, void *value);

#define DArray_last(A) ((A)->contents[(A)->end - 1])
#define DArray_first(A) ((A)->contents[0])
#define DArray_end(A) ((A)->end)
#define DArray_count(A) ((A)->count)
#define DArray_capacity(A) ((A)->capacity)

#define DArray_free_element(E) FREE(E)

static inline void* DArray_new_element(DArray *array) {
    
	check(array->element_size > 0,
            "Can't use DArray_new on 0 size darrays.");

    return CALLOC(1, array->element_size, void);

error:
    return NULL;
}

static inline void DArray_set(DArray * array, ssize_t i, void *element) {
   
    check(i < array->capacity, "darray attempt to set past capacity");
    
	if (i >= array->end) {
        array->end = i;
		array->count++;
		array->end++;
	}

	void *el = array->contents[i];
	DArray_free_element(el);

	array->contents[i] = DArray_new_element(array);
	check_mem(array->contents[i]);
    memcpy(array->contents[i], element, array->element_size);

error:
    return;
}

static inline void* DArray_get(DArray * array, ssize_t i) {
 
    check(i < array->capacity, "darray attempt to get past capacity");
    return array->contents[i];

	error:
    	return NULL;
}

static inline void DArray_shift_left(DArray *array, ssize_t i) {

	for (ssize_t index = i ; index < DArray_count(array) - 1; index++) {
		array->contents[index] = array->contents[index + 1];
	}
}

static inline void DArray_remove(DArray * array, ssize_t i) {
    
	void *element = array->contents[i];

	DArray_shift_left(array, i);
	
	DArray_free_element(element);
    array->contents[array->end - 1] = NULL;
	array->end--;
	array->count--;
}

#endif