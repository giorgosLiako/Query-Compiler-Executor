#include <stdio.h>
#include <stdlib.h>
#include "DArray.h"

DArray* DArray_create(size_t element_size, int32_t initial_capacity) {

	DArray *array = MALLOC(DArray, 1);
	check_mem(array);
	array->capacity = initial_capacity;
	check(array->capacity > 0, "You must set an initial capacity > 0");

	array->contents = CALLOC(initial_capacity, sizeof(void *), void *);
	check_mem(array->contents);

	array->end = 0;
	array->count = 0;
	array->element_size = element_size;

	return array;

	error:
		FREE(array);
		return NULL;
}

void DArray_clear(DArray *array) {

	if (array->element_size > 0) {
		for (size_t i = 0 ; i < DArray_count(array) ; i++) {
			FREE(array->contents[i]);
		}
	}
	array->end = 0;
	array->count = 0;
}

void DArray_destroy(DArray *array) {

	if (array) {
		DArray_clear(array);
		FREE(array->contents);
		FREE(array);
	}
}

int DArray_resize(DArray *array, int32_t newsize) {

	array->capacity = newsize;
	check(array->capacity > 0, "The new size must be > 0");

	void **contents = REALLOC(array->contents, array->capacity, void *);
	check_mem(contents);

	array->contents = contents;
	return 0;

	error:
		return -1;
}

int DArray_expand(DArray *array) {

	return DArray_resize(array, array->capacity * 2);
}

int DArray_push(DArray *array, void *element) {

	array->contents[array->end] = DArray_new_element(array);
	check_mem(array->contents[array->end]);
	memcpy(array->contents[array->end], element, array->element_size);

	array->end++;
	array->count++;

	if (DArray_end(array) >= DArray_capacity(array)) {
		return DArray_expand(array);
	}
	
	return 0;

	error:
		return -1;
}

void* DArray_pop(DArray *array) {

	check(array->end - 1 >= 0, "Attempt to pop from empty array");

	void *element = array->contents[array->end - 1];
	array->contents[array->end - 1] = NULL;
	array->end--;
	array->count--;

	if (array->end < array->capacity / 2 && (array->end + 1 % array->capacity / 2 == 0)) {
		DArray_resize(array, array->capacity / 2);
	}

	return element;
	error:
		return NULL;
}
