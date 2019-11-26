#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"



int main(int argc, char **argv) {

    relation R, S;
    //check id a macro on dbg.h 
    check(argc == 5, "You should provide 2 input files, corrent syntax is : ./sort_merge <path to input file (1)> <path to input file (2)>");

    //take the two relations and do sort merge join
    SortMergeJoin(&R, &S);

    //free the allocated memory and exit
    FREE(S.tuples);
    FREE(R.tuples);
   
    return EXIT_SUCCESS;

    error:
        FREE(S.tuples);
        FREE(R.tuples);
      
        return EXIT_FAILURE;
}
