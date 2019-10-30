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

    R.num_tuples = 1000;
    R.tuples = MALLOC(tuple, R.num_tuples);

    S.num_tuples = 10;
    S.tuples = MALLOC(tuple, S.num_tuples);

    srand(328398329);

    log_info("ARRAY: KEY\tROW_ID\n");

    for (size_t i = 0 ; i < 1000 ; i++) {
        uint64_t key = 0;
        for (int j=0; j<64; j++) {
            key = key*2 + rand()%2;
        }
        R.tuples[i].key = key;
        R.tuples[i].payload = i;

    	printf("%lu\t%lu\n", key, i);
    }

    /*for (size_t i = 0 ; i < 10 ; i++) {
        uint64_t key = rand() % 10;
        R.tuples[i].key = key;
        R.tuples[i].payload = i;
        printf("%lu\t%lu\n", key, i);

    }*/

    SortMergeJoin(&R, &S);

    FREE(R.tuples);
    FREE(S.tuples);

    return 0;
}

