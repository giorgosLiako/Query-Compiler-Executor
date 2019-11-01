#define _GNU_SOURCE
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/sort_merge.h"
#include "../src/structs.h"
#include "../src/dbg.h"


int main(int argc, char **argv) {

    check(argc == 3, "You should provide 2 input files, corrent syntax is : ./sort_merge <path to input file (1)> <path to input file (2)>");

    size_t lines;
    relation R;
    check(count_lines(argv[1], &lines) != -1, "Something went wrong in reading the input file!");
    check(parse_file(argv[1], lines, &R) != -1, "Something went wrong in parsing the input file!");

    relation S;
    check(count_lines(argv[2], &lines) != -1, "Something went wrong in reading the input file!");
    check(parse_file(argv[2], lines, &S) != -1, "Something went wrong in parsing the input file!");

    sort_merge_join()

    return EXIT_SUCCESS;

    error:
        return EXIT_FAILURE;
}


    // fp = fopen(argv[1], "r");
    // check(fp != NULL, "Failed to open the file!");

    // size_t lines = 0, n = 0;
    // char *lineptr;
    // ssize_t nread;

    // while ((nread = getline(&lineptr, &n, fp)) != -1) {
    //     lines++;
    // }
    // lines = count_lines();

    // debug("lines_read = %lu", lines);

    // rewind(fp);

    // while ((nread = getline(&lineptr, &n, fp)) != -1) {
        
    // }

