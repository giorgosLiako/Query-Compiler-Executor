#define _GNU_SOURCE
#include <stdio.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "alloc_free.h"
#include "io_functions.h"
#include "structs.h"

//struct for CUnit
typedef struct IO_test {
    FILE *fp;
    char *filename;
    size_t lines;
    char *lineptr;
    size_t n;
    ssize_t nread;
    relation *rel;
} IO_test;

IO_test io_test_var;


//function for CUnit : opens a file for read
int file_open_read() {

    if (NULL == (io_test_var.fp = fopen(io_test_var.filename, "r"))) {
        return -1;
    }
    else {
        return 0;
    }
}

//function for CUnit : opens a file for write
int file_open_write() {

    if (NULL == (io_test_var.fp = fopen(io_test_var.filename, "w"))) {
        return -1;
    }
    else {
        return 0;
    }
}

//function for CUnit : closes a file
int close_file() {
    if (0 != fclose(io_test_var.fp)) {
        return -1;
    }
    else {
        io_test_var.fp = NULL;
        return 0;
    }
}

//function for CUnit : counts the lines of a file
void file_read_lines() {

    if (NULL != io_test_var.fp) {
        while ((io_test_var.nread = getline(&io_test_var.lineptr, &io_test_var.n, io_test_var.fp)) != -1) {
            io_test_var.lines++;
        }
    }
    FREE(io_test_var.lineptr);
}

//function that counts the lines of a file
int count_lines(char *fname, size_t *lin) {

    CU_pSuite pSuite = NULL;
    //initilize CU suite
    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }
    //open the file
    pSuite = CU_add_suite("File_handle_suite", file_open_read, close_file);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }
    //allocate space for the filename
    io_test_var.filename = MALLOC(char, strlen(fname) + 1);
    strncpy(io_test_var.filename, fname, strlen(fname) + 1);
    //call the CU function that counts the lines
    if (NULL == CU_add_test(pSuite, "test for counting file lines", file_read_lines)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.lines = 0;
    io_test_var.n = 0;
    io_test_var.lines = 0;
    io_test_var.lineptr = NULL;
    //run the test
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    //save the data
    *lin = io_test_var.lines;

    FREE(io_test_var.filename);
    return CU_get_error();
}

//function for CUnit : parses the file
void file_parse() {

    io_test_var.rel->num_tuples = io_test_var.lines;
    io_test_var.rel->tuples = MALLOC(tuple, io_test_var.lines);
    CU_ASSERT(io_test_var.rel->tuples != NULL);
    //parses the file and save the data of the relation
    size_t i  = 0;
    while ((io_test_var.nread = getline(&io_test_var.lineptr, &io_test_var.n, io_test_var.fp)) != -1) {
        CU_ASSERT(2 == sscanf(io_test_var.lineptr, "%lu,%lu\n", &(io_test_var.rel->tuples[i].key), &(io_test_var.rel->tuples[i].payload)));
        i++;
    }

    FREE(io_test_var.lineptr);
}

//function that parses the file
int parse_file(char *fname, size_t lin, relation *r) {

    CU_pSuite pSuite = NULL;
    //initialize CU suite
    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }
    //open the file 
    pSuite = CU_add_suite("File_handle_suite", file_open_read, close_file);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }
    //allocate space for the filename
    io_test_var.filename = MALLOC(char, strlen(fname) + 1);
    strncpy(io_test_var.filename, fname, strlen(fname) + 1);
    //call the CU function that parses the file
    if (NULL == CU_add_test(pSuite, "test for parsing the file input", file_parse)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    
    io_test_var.lines = lin;
    io_test_var.lineptr = NULL;
    io_test_var.n = 0;
    io_test_var.rel = r;
    
    //run the test
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    //save the data to the relation
    r->num_tuples = io_test_var.rel->num_tuples;
    r->tuples = io_test_var.rel->tuples;

    FREE(io_test_var.filename);
    return CU_get_error();
}
