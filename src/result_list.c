#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include "alloc_free.h"
#include "dbg.h"
#include "result_list.h"

result *create_result_list() {

    result *list = MALLOC(result, 1);
    check_mem(list);
    list->head = NULL;

    return list;

    error:
        return NULL;
}

void destroy_result_list(result *list) {

    if (!list) {
        return;
    }
    if (!list->head) {
        FREE(list);
        return;
    }

    list_node *current = list->head;
    list_node *next;

    while (current->next != NULL) {
        next = current->next;
        current->next = current->next->next;
        FREE(next);
    }
    FREE(current);
    FREE(list);
}

int add_to_result_list(result *list, uint64_t row_id_1, uint64_t row_id_2) {

    row_ids r_ids;
    r_ids.row_id_1 = row_id_1;
    r_ids.row_id_2 = row_id_2;

    if (list->head) {
        list_node *tmp = list->head;
        list_node *prev;
        while (tmp != NULL) {
            if (tmp->count < BUF_SIZE) {
                tmp->buffer[tmp->count++] = r_ids;
                return 0;
            }
            prev = tmp;
            tmp = tmp->next;
        }
        //We didnt find any empty place, allocate new
        prev->next = MALLOC(list_node, 1);
        check_mem(prev->next);
       
        prev->next->count = 0;
        prev->next->buffer[prev->next->count++] = r_ids;
        prev->next->next = NULL;
        
    } else {
        list->head = MALLOC(list_node, 1);
        check_mem(list->head);

        list->head->next = NULL;
        list->head->count = 0;
        list->head->buffer[list->head->count++] = r_ids;
    }

    return 0;

    error:
        return -1;
}
