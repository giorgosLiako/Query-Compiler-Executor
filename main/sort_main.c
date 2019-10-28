#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/sort_merge.h"
#include "../src/structs.h"


int main() {

    relation R;
    relation S;

    S.num_tuples = 0;
    
    R.tuples = malloc(sizeof(tuple) * 10);
    R.num_tuples = 10;

    srand(time(NULL));

    for (size_t i = 0 ; i < 10 ; i++) {
        int64_t key = rand() % 10;
        R.tuples[i].key = key;
        R.tuples[i].payload = i;
    }

    SortMergeJoin(&R, &S);

    return 0;
}

