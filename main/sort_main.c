#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"


int main() {

    relation R;
    relation S;


    R.num_tuples = 1000000;
    R.tuples = MALLOC(tuple, R.num_tuples);

    S.num_tuples = 1000000;
    S.tuples = MALLOC(tuple, S.num_tuples);

    srand(32839832);

  //  log_info("ARRAY: KEY\tROW_ID\n");

        // R.tuples[0].key = 1;
        // R.tuples[0].payload = 0;

        // R.tuples[1].key = 1;
        // R.tuples[1].payload = 1;

        // S.tuples[0].key = 1;
        // S.tuples[0].payload = 0;

        // S.tuples[1].key = 1;
        // S.tuples[1].payload = 1;

    for (size_t i = 2 ; i < R.num_tuples ; i++) {
        uint64_t key = 0;
        for (int j=0; j<64; j++) {
            key = key*2 + rand()%2;
        }
        
        // uint64_t key = rand()%10;
        R.tuples[i].key = key;
        R.tuples[i].payload = i;

    	//printf("%lu\t%lu\n", key, i);
    }

    for (size_t i = 2 ; i < S.num_tuples ; i++) {
        uint64_t key = 0;
        for (int j=0; j<64; j++) {
            key = key*2 + rand()%2;
        }
        
        // uint64_t key = rand()%10;
        S.tuples[i].key = key;
        S.tuples[i].payload = i;

    	//printf("%lu\t%lu\n", key, i);
    }

    SortMergeJoin(&S, &R);

    FREE(R.tuples);
    FREE(S.tuples);

    return 0;
}

