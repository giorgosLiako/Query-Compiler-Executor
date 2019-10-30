#include <stdio.h>
#include <stdlib.h>
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


relation * build_reordered_array(relation* reorder_rel , relation *prev_rel, histogram* histo , histogram* psum, int wanted_byte )
{
    for (size_t i = 0 ; i < prev_rel->num_tuples ; i++)
    {
        uint32_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t start = psum->hist[byte];
        size_t end = start + histo->hist[byte ] ; 

        for (size_t j = start  ; j < end ; j++)
        {
            if ( reorder_rel->tuples[j].key < 0)
            {
                reorder_rel->tuples[j].key = prev_rel->tuples[i].key;
                reorder_rel->tuples[j].payload = prev_rel->tuples[i].payload;
                break;
            }
        }
    }

    return reorder_rel;
}


relation* allocate_reordered_array(relation* rel)
{
    relation *r = NULL;
    r = malloc(sizeof(relation));
    check_mem(r);

    r->num_tuples = rel->num_tuples;
    r->tuples = malloc( rel->num_tuples *  sizeof(tuple));
    check_mem(r->tuples);

    for (size_t i = 0 ; i < rel->num_tuples ; i++)
        r->tuples[i].key = -1;

    return r;

    error:
        return NULL;
}

void free_reordered_array(relation* r , relation * s)
{
    free(r->tuples);
    free(r);

    free(s->tuples);
    free(s);
}

result* SortMergeJoin(relation *relR, relation *relS) {

    histogram histR, histS;

    histogram psumR, psumS;

    int byte = 8;

    build_histogram(relR, &histR, byte);
    build_histogram(relS, &histS, byte); 

    debug("AFTER BUILDING HISTOGRAMS");

    for (ssize_t i = 0 ; i < 256 ; i++) {
        if (histR.hist[i] != 0)
            printf("%ld : %d\n", i, histR.hist[i]);
    }

    build_psum(&histR, &psumR);
    build_psum(&histS, &psumS);

    debug("AFTER BUILDING PSUMS");

    for (ssize_t i  = 0 ; i < 256 ; i++) {
        if (psumR.hist[i] != -1)
          printf("%ld : %d\n", i, psumR.hist[i]); 
    }

    relation * reorderedR = NULL;
    relation * reorderedS = NULL;

    reorderedR = allocate_reordered_array(relR);
    reorderedS = allocate_reordered_array(relS);

    reorderedR = build_reordered_array(reorderedR,relR , &histR , &psumR , byte);
    reorderedS = build_reordered_array(reorderedS,relS , &histS , &psumS , byte);

    debug("REORDERED ARRAY: ");
    debug("KEY    ROW_ID\n");
    for (size_t i = 0 ; i < reorderedR->num_tuples ; i++) {
        printf("%ld\t%ld\n",reorderedR->tuples[i].key , reorderedR->tuples[i].payload );
    }

    free_reordered_array(reorderedR , reorderedS);

    return NULL;
}