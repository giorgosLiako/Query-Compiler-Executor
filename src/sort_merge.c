#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "structs.h"
#include "result_list.h"
#include "dbg.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *rel, histogram *hist, int wanted_byte) {

    for (size_t i = 0 ; i < 256 ; i++) {
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
    size_t j = 0;
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

    histogram histR, histS;

    histogram psumR, psumS;

    int byte = 8;

    relation *reorderedR = NULL;
    relation *reorderedS = NULL;

    reorderedR = allocate_reordered_array(relR);
    reorderedS = allocate_reordered_array(relS);

    build_histogram(relR, &histR, byte);
    build_histogram(relS, &histS, byte); 

    log_info("Histogram");

    for (size_t i = 0 ; i < 256 ; i++) {
        if (histR.hist[i] != 0) {
            printf("%lu %d\n", i, histR.hist[i]);
        }
    }

    build_psum(&histR, &psumR);
    build_psum(&histS, &psumS);

    log_info("Psum");

    for (size_t i = 0 ; i < 256 ; i++) {
        if (psumR.hist[i] != -1) {
            printf("%lu %d\n", i, psumR.hist[i]);
        }
    }

    reorderedR = build_reordered_array(reorderedR,relR , &histR , &psumR , byte);
    reorderedS = build_reordered_array(reorderedS,relS , &histS , &psumS , byte);

    log_info("AFTER REORDERED");

    for (size_t i = 0 ; i < reorderedR->num_tuples ; i++) {
        printf("%lu\t%lu\n", reorderedR->tuples[i].key, reorderedR->tuples[i].payload);
    }

    free_reordered_array(reorderedR);
    free_reordered_array(reorderedS);

    return NULL;
}