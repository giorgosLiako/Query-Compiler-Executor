#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"


int main() {

    relation R;
    relation S;

    S.num_tuples = 0;
    
    
    
    R.tuples = malloc(sizeof(tuple) * 10);
    check_mem(R.tuples);
    R.num_tuples = 10;

    srand(328398329);

    printf("ARAY: \nKEY\tROW_ID\n");

    for (size_t i = 0 ; i < 10 ; i++) {
        uint64_t key = 0;
        for (int j=0; j<64; j++) {
            key = key*2 + rand()%2;
        }
        R.tuples[i].key = key;
        R.tuples[i].payload = i;
    	printf("%ld\t%ld\n",key , i );
    }



    SortMergeJoin(&R, &S);

    free(R.tuples);

    return 0;
}

