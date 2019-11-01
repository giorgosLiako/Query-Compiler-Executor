#include "utilities.h"

void build_histogram(relation *rel, histogram *hist, uint8_t wanted_byte, int start, int size) {

    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }

    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->array[byte]++;
    }
}

void build_psum(histogram *hist, histogram *psum) {

    for (size_t i =  0 ; i < 256 ; i++) {
        psum->array[i] = -1;
    }

    size_t offset = 0;
    for (ssize_t i = 0;  i < 256 ; i++) {
        if (hist->array[i] != 0) {
            psum->array[i] = offset;
            offset +=  hist->array[i];
        }
    }

}

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

void free_reordered_array(relation* r) {
    FREE(r->tuples);
    FREE(r);
}

void swap_arrays(relation* r1, relation* r2) {
    relation* temp;
    temp = r1;
    r1 = r2;
    r2 = temp;
}

bool is_sorted(relation *relR, uint64_t num_tuples) {

    for (size_t i = 1 ; i < num_tuples ; i++) {
        if (relR->tuples[i].key < relR->tuples[i - 1].key) {
            return false;
        }
    }

    return true;
}