#ifndef RESULT_LIST_H
#define RESULT_LIST_H

#include <stdint.h>

#define BUF_SIZE 65536

typedef struct row_ids {
    uint64_t row_id_1;
    uint64_t row_id_2;
} row_ids;

typedef struct list_node{
    row_ids buffer[BUF_SIZE];
    uint32_t count;
    struct list_node *next;
} list_node;

typedef struct result {
    list_node *head;
} result;

result *create_result_list();

void destroy_result_list(result *);

int add_to_result_list(result *, uint64_t, uint64_t);

#endif