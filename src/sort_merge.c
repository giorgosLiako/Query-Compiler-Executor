#include <stdio.h>
#include <stdlib.h>
#include "structs.h"
#include "result_list.h"
#include "dbg.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histograms(relation *relR, relation *relS, histogram *histR, histogram *histS, int wanted_byte) {

    for (ssize_t i = 0 ; i < 256 ; i++) {
        histR->hist[i] = 0;
        histS->hist[i] = 0;
    }

    for (size_t i = 0 ; i < relR->num_tuples ; i++) {
        uint32_t byte = get_byte(relR->tuples[i].key, wanted_byte);
        histR->hist[byte]++;
    }

    for (size_t i = 0 ; i < relS->num_tuples ; i++) {
        uint32_t byte = get_byte(relS->tuples[i].key, wanted_byte);
        histS->hist[byte]++;
    }
}

void build_psums(histogram *histR, histogram *histS, histogram *psumR, histogram *psumS) {

    for (size_t i =  0 ; i < 256 ; i++) {
        psumR->hist[i] = -1;
        psumS->hist[i] = -1;
    }

    int32_t non_zeroR[256];

    size_t j = 0;
    for (size_t i = 0 ; i < 256 ; i++) {
        if (histR->hist[i] != 0) {
            non_zeroR[j++] = i;
        }
    }

    psumR->hist[non_zeroR[0]] = 0;
    for (size_t i = 1 ; i < j ; i++) {
        psumR->hist[non_zeroR[i]] = psumR->hist[non_zeroR[i - 1]] + histR->hist[non_zeroR[i - 1]];
    }

    int32_t non_zeroS[256];

    j = 0;
    for (size_t i = 0 ; i < 256 ; i++) {
        if (histS->hist[i] != 0) {
            non_zeroS[j++] = i;
        }
    }

    psumS->hist[non_zeroS[0]] = 0;
    for (size_t i = 1 ; i < j ; i++) {
        psumS->hist[non_zeroS[i]] = psumS->hist[non_zeroS[i - 1]] + histS->hist[non_zeroR[i - 1]];
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
    if ( r == NULL)
    {
        printf("Kosta ftiakse macro gia debug\n");
    }
    r->num_tuples = rel->num_tuples;

    r->tuples = malloc( rel->num_tuples *  sizeof(tuple));
    if ( r->tuples  == NULL)
    {
        printf("Kosta ftiakse macro gia debug\n");
    }    

    for (size_t i = 0 ; i < rel->num_tuples ; i++)
        r->tuples[i].key = -1;

    return r;
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

    build_histograms(relR, relS, &histR, &histS , byte);

    debug("AFTER BUILDING HISTOGRAMS");

    for (ssize_t i = 0 ; i < 256 ; i++) {
        if (histR.hist[i] != 0)
            printf("%ld : %d\n", i, histR.hist[i]);
    }

    build_psums(&histR, &histS, &psumR, &psumS);

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
    printf("KEY    ROW_ID\n");
    for (size_t i = 0 ; i < reorderedR->num_tuples ; i++) {
        printf("%ld\t%ld\n",reorderedR->tuples[i].key , reorderedR->tuples[i].payload );
    }

    free_reordered_array(reorderedR , reorderedS);

    return NULL;
}