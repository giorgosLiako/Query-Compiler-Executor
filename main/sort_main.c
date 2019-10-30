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

    SortMergeJoin(&R, &S);

    free(R.tuples);

    return 0;
}

