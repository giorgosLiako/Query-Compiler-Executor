#define _GNU_SOURCE
#include <stdio.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "alloc_free.h"
#include "io_functions.h"
#include "structs.h"
#include "result_list.h"


typedef struct IO_test {
    FILE *fp;
    char *filename;
    size_t lines;
    char *lineptr;
    size_t n;
    ssize_t nread;
    relation *rel;
    result *res_list;
} IO_test;

IO_test io_test_var;



int file_open_read() {

    if (NULL == (io_test_var.fp = fopen(io_test_var.filename, "r"))) {
        return -1;
    }
    else {
        return 0;
    }
}

int file_open_write() {

    if (NULL == (io_test_var.fp = fopen(io_test_var.filename, "w"))) {
        return -1;
    }
    else {
        return 0;
    }
}

int close_file() {
    if (0 != fclose(io_test_var.fp)) {
        return -1;
    }
    else {
        io_test_var.fp = NULL;
        return 0;
    }
}

void file_read_lines() {

    if (NULL != io_test_var.fp) {
        while ((io_test_var.nread = getline(&io_test_var.lineptr, &io_test_var.n, io_test_var.fp)) != -1) {
            io_test_var.lines++;
        }
    }
    FREE(io_test_var.lineptr);
}

int count_lines(char *fname, size_t *lin) {

    CU_pSuite pSuite = NULL;

    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }

    pSuite = CU_add_suite("File_handle_suite", file_open_read, close_file);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.filename = MALLOC(char, strlen(fname) + 1);
    strncpy(io_test_var.filename, fname, strlen(fname) + 1);

    if (NULL == CU_add_test(pSuite, "Test for counting file lines", file_read_lines)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.lines = 0;
    io_test_var.n = 0;
    io_test_var.lineptr = NULL;

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    *lin = io_test_var.lines;

    FREE(io_test_var.filename);
    return CU_get_error();
}

void file_parse() {

    io_test_var.rel->num_tuples = io_test_var.lines;
    io_test_var.rel->tuples = MALLOC(tuple, io_test_var.lines);
    CU_ASSERT(io_test_var.rel->tuples != NULL);
    
    size_t i  = 0;
    while ((io_test_var.nread = getline(&io_test_var.lineptr, &io_test_var.n, io_test_var.fp)) != -1) {
        CU_ASSERT(2 == sscanf(io_test_var.lineptr, "%lu,%lu\n", &(io_test_var.rel->tuples[i].key), &(io_test_var.rel->tuples[i].payload)));
        i++;
    }

    FREE(io_test_var.lineptr);
}

int parse_file(char *fname, size_t lin, relation *r) {

    CU_pSuite pSuite = NULL;

    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }

    pSuite = CU_add_suite("File_handle_suite", file_open_read, close_file);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.filename = MALLOC(char, strlen(fname) + 1);
    strncpy(io_test_var.filename, fname, strlen(fname) + 1);

    if (NULL == CU_add_test(pSuite, "test for parsing the file input", file_parse)) {
        CU_cleanup_registry();
        return CU_get_error();
    }


    io_test_var.lines = lin;
    io_test_var.lineptr = NULL;
    io_test_var.n = 0;
    io_test_var.rel = r;
    
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    r->num_tuples = io_test_var.rel->num_tuples;
    r->tuples = io_test_var.rel->tuples;

    FREE(io_test_var.filename);
    return CU_get_error();
}

void file_write() {
    
    list_node *current = io_test_var.res_list->head;
    while (current != NULL) {
        for (size_t i = 0 ; i < current->count ; i++) {
            CU_ASSERT(0 != fprintf(io_test_var.fp, "%lu,%lu\n", current->buffer[i].row_id_1, current->buffer[i].row_id_2)); 
        }
        current = current->next;
    }
}

int write_to_file(result *res) {

    CU_pSuite pSuite = NULL;

    if (CUE_SUCCESS != CU_initialize_registry()) {
        return CU_get_error();
    }

    pSuite = CU_add_suite("File_handle_suite", file_open_write, close_file);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.filename = MALLOC(char, strlen("results.txt") + 1);
    strncpy(io_test_var.filename, "results.txt", strlen("results.txt") + 1);

    if (NULL == CU_add_test(pSuite, "Test for file writing", file_write)) {
        CU_cleanup_registry();
        return CU_get_error();
    }

    io_test_var.res_list = res;

    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    FREE(io_test_var.filename);
    return CU_get_error();
}