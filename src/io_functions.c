#define _GNU_SOURCE
#include <stdio.h>
#include "alloc_free.h"
#include "io_functions.h"
#include "structs.h"
#include "result_list.h"

int count_lines(char *filename, size_t *lin) {

    FILE *fp = fopen(filename, "r");
    check(fp != NULL, "Failed to open the file!");

    size_t lines = 0, n = 0;
    char *lineptr = NULL;
    ssize_t nread;

    while ((nread = getline(&lineptr, &n, fp)) != -1) {
        lines++;
    }

    *lin = lines;

    fclose(fp);
    FREE(lineptr);
    return 0;

    error:
        return -1;
}

int parse_file(char *filename, size_t lines, relation *rel) {

    FILE *fp = fopen(filename, "r");
    check(fp != NULL, "Failed to open the file!");

    size_t n = 0;
    char *lineptr = NULL;
    ssize_t nread;

    rel->num_tuples = lines;
    rel->tuples = MALLOC(tuple, lines);

    size_t i  = 0;
    while ((nread = getline(&lineptr, &n, fp)) != -1) {
        sscanf(lineptr, "%lu,%lu\n", &(rel->tuples[i].key), &(rel->tuples[i].payload));
        i++;
    }

    fclose(fp);
    FREE(lineptr);

    return 0;

    error:
        return -1;
}

int write_to_file(result *res_list) {

    const char *filename = "results.txt";

    FILE *fp = fopen(filename, "w");
    check(fp != NULL, "Couldn't open file for writing");

    list_node *current = res_list->head;
    while (current != NULL) {
        for (size_t i = 0 ; i < current->count ; i++) {
            fprintf(fp, "%lu,%lu\n", current->buffer[i].row_id_1, current->buffer[i].row_id_2);
        }
        current = current->next;
    }

    fclose(fp);
    return 0;
    
    error:
        fclose(fp);
        return -1;
}
