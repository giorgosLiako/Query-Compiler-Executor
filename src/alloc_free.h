#ifndef alloc_free_h
#define alloc_free_h

#define MALLOC(type, size) ( (type*) malloc((size) * sizeof(type)))

#define CALLOC(items, size, type) ( (type*) calloc(items, size))

#define REALLOC(pointer, size, type) ( (type *) realloc(pointer , (size) * sizeof(*(pointer))))

#define FREE(pointer) do { if ((pointer)) { free(pointer); pointer = NULL; } } while(0)

#endif