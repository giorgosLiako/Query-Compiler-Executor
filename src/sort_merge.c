#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "DArray.h"
#include "structs.h"
#include "result_list.h"
#include "dbg.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *rel, histogram *hist, int wanted_byte) {

    for (ssize_t i = 0 ; i < 256 ; i++) {
        hist->hist[i] = 0;
    }

    for (size_t i = 0 ; i < rel->num_tuples ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->hist[byte]++;
    }
}

void build_psum(histogram *hist, histogram *psum) {

    for (size_t i =  0 ; i < 256 ; i++) {
        psum->hist[i] = -1;
    }

    size_t offset = 0;
    for (size_t i = 0; i < 256; i++) {
        if (hist->hist[i] != 0) {
            psum->hist[i] = offset;
            offset +=  hist->hist[i];
        }
    }

}


relation* build_reordered_array(relation* reorder_rel , relation *prev_rel, 
                                histogram* histo , histogram* psum, 
                                int wanted_byte ) {
   
    histogram temp = *histo;

    
    for (size_t i = 0 ; i < prev_rel->num_tuples ; i++) {
        uint32_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t index = psum->hist[byte] + (histo->hist[byte] - temp.hist[byte]);

        temp.hist[byte]--;    

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

result* SortMergeJoin(relation *relR, relation *relS) {

    int byte = 1;

    relation *reorderedR = NULL;

    reorderedR = allocate_reordered_array(relR);

    stack_node s_node;

    DArray *stack = DArray_create(sizeof(stack_node), 25);

    while (1) {

        s_node.hist = MALLOC(histogram, 1);
        check_mem(s_node.hist);
        s_node.psum = MALLOC(histogram, 1);
        check_mem(s_node.psum);

        DArray_push(stack, &s_node);

        build_histogram(relR, s_node.hist, byte);
        build_psum(s_node.hist, s_node.psum);

        reorderedR = build_reordered_array(reorderedR, relR , s_node.hist, s_node.psum, byte);
    }

    free_reordered_array(reorderedR);
    
    error:
        free_reordered_array(reorderedR);
        FREE(s_node.hist);
        FREE(s_node.psum);
        return NULL;    
}