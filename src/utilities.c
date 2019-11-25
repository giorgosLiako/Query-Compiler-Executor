#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "utilities.h"
#include "histogram.h"

//function to build a histogram
void build_histogram(relation *rel, histogram *hist, uint8_t wanted_byte, int start, int size) {
    //start is the index that starts the bucket and size is the number of tuples that has
    //intialize the histogram
    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }
    //take the wanted byte of the tuples that belong to the bucket and make the histogram
    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->array[byte]++; 
    }
}

//function to build psum
void build_psum(histogram *hist, histogram *psum) {
    //initialize the psum
    for (size_t i =  0 ; i < 256 ; i++) {
        psum->array[i] = -1;
    }

    size_t offset = 0;
    for (ssize_t i = 0;  i < 256 ; i++) {
        if (hist->array[i] != 0) { //every cell is not 0 there is in the histogram , so put it in the psum
            psum->array[i] = offset; 
            offset +=  hist->array[i]; //find the offset with the help of histogram
        }
    }

}

//function to build the reordered array
relation* build_reordered_array(relation* reorder_rel , relation *prev_rel, 
                                histogram* histo , histogram* psum, 
                                uint8_t wanted_byte, int start, int size) {
   
    histogram temp = *histo;

    
    for (ssize_t i = start ; i < start + size ; i++) {
        uint8_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t index = start +  psum->array[byte] + (histo->array[byte] - temp.array[byte]);

        temp.array[byte]--;    

        reorder_rel->tuples[index].key = prev_rel->tuples[i].key;
        reorder_rel->tuples[index].payload = prev_rel->tuples[i].payload;
    }

    return reorder_rel;
}


//function to allocate the memory for the reordered array
relation* allocate_reordered_array(relation* rel) {
    
    relation *r = NULL;
    r = MALLOC(relation, 1);
    check_mem(r);

    r->num_tuples = rel->num_tuples;
    r->tuples = MALLOC(tuple, rel->num_tuples);
    check_mem(r->tuples);

    return r;

    error:
        return NULL;
}

//function to free the allocated memory of the reordered array
void free_reordered_array(relation* r) {
    FREE(r->tuples);
    FREE(r);
}

//function to swap arrays
void swap_arrays(relation* r1, relation* r2) {
    relation* temp;
    temp = r1;
    r1 = r2;
    r2 = temp;
}


static inline void fill_arrays(metadata_array *arr, uint64_t *mapped_file) {
    
    arr->tuples = *mapped_file++;
    arr->columns = *mapped_file++;
    arr->data = MALLOC(relation *, arr->columns);

    for (size_t j = 0 ; j < arr->columns ; j++) {
        relation *rel = MALLOC(relation, 1);
        rel->num_tuples = arr->tuples;
        rel->tuples = MALLOC(tuple, arr->tuples);
        arr->data[j] = rel;
        for (size_t i = 0 ; i < arr->tuples ; i++) {
            rel->tuples[i].key = *mapped_file++;
            rel->tuples[i].payload = i;
        }
    }
}


int read_relations(metadata_array *arr) {

    char *linptr = NULL;
    size_t n = 0;

    while (getline(&linptr, &n, stdin) != -1) {
        if (!strncmp(linptr, "Done\n", strlen("Done\n")) || !strncmp(linptr,"done\n", strlen("done\n"))) {
            break;
        }
        
        linptr[strlen(linptr) - 1] = '\0';
        int fd = open(linptr, O_RDONLY, S_IRUSR | S_IWUSR);
        check(fd != -1, "open failed");

        struct stat sb;

        check(fstat(fd, &sb) != -1, "fstat failed");

        uint64_t *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        check(mapped_file != MAP_FAILED, "mmap failed");

        fill_arrays(arr, mapped_file);

        debug("printing arrays for debugging");

        for (size_t j = 0 ; j < arr->columns ; j++) {
            for (size_t i = 0 ; i < arr->tuples ; i++) {
                debug("%lu", arr->data[j]->tuples[i].key);
            }
            debug("");
        }

        munmap(mapped_file, sb.st_size);
        close(fd);
    }

    FREE(linptr);
    return 0;

    error:
        FREE(linptr);
        return -1;
}