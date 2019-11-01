#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/io_functions.h"
#include "../src/dbg.h"


int main(int argc, char **argv) {

    relation R, S;
    result *result_list = NULL;
    check(argc == 3, "You should provide 2 input files, corrent syntax is : ./sort_merge <path to input file (1)> <path to input file (2)>");

    size_t lines;

    check(count_lines(argv[1], &lines) != -1, "Something went wrong in reading the input file!");
    check(parse_file(argv[1], lines, &R) != -1, "Something went wrong in parsing the input file!");

    check(count_lines(argv[2], &lines) != -1, "Something went wrong in reading the input file!");
    check(parse_file(argv[2], lines, &S) != -1, "Something went wrong in parsing the input file!");

    result_list = SortMergeJoin(&R, &S);

    check(write_to_csv(result_list) != -1, "Something went wrong in writing at the output file!");

    destroy_result_list(result_list);
    FREE(S.tuples);
    FREE(R.tuples);
   
    return EXIT_SUCCESS;

    error:
        destroy_result_list(result_list);
        FREE(S.tuples);
        FREE(R.tuples);
      
        return EXIT_FAILURE;
}
