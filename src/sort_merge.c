#include <stdio.h>
#include "structs.h"
#include "result_list.h"
#include "dbg.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histograms(relation *relR, relation *relS, histogram *histR, histogram *histS) {

    for (ssize_t i = 0 ; i < 256 ; i++) {
        histR->hist[i] = 0;
        histS->hist[i] = 0;
    }

    for (size_t i = 0 ; i < relR->num_tuples ; i++) {
        uint32_t byte = get_byte(relR->tuples[i].key, 8);
        histR->hist[byte]++;
    }

    for (size_t i = 0 ; i < relS->num_tuples ; i++) {
        uint32_t byte = get_byte(relS->tuples[i].key, 8);
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

result* SortMergeJoin(relation *relR, relation *relS) {

    histogram histR, histS;

    histogram psumR, psumS;

    build_histograms(relR, relS, &histR, &histS);

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

    return NULL;
}