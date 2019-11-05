#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "queue.h"
#include "dbg.h"

//function to initialize a queue
queue* new_queue() {
    queue* new_q = MALLOC(queue, 1);
    check_mem(new_q);
    
    new_q->first = NULL;
    new_q->last = NULL;
    new_q->size = 0;

    return new_q;

    error:
        return NULL;
}

//function to allocate memory and set a new item 
item* new_item(ssize_t base, ssize_t size) {
    item* new_i = MALLOC(item, 1);
    check_mem(new_i);

    new_i->base = base;
    new_i->size = size;

    return new_i;

    error:
        return NULL;

}

//function to delete an item of the queue
void delete_item(item* i) {
    FREE(i);
    return;
}

//function to insert a new item in the queue
void push(queue* q, item* new_item) {
    if (new_item == NULL) return;

    node* new_n = MALLOC(node, 1);
    check_mem(new_n);

    new_n->value = new_item;
    new_n->next = NULL;

    if (q->size == 0) {
        q->first = new_n;
        q->last = new_n;
        q->size++;
    }
    else {
        q->last->next = new_n;
        q->last = new_n;
        q->size++;
    }

    error:
        return;
}

//function to pop an item of the queue
item* pop(queue *q){
    if (q->size == 0) goto error;
    item* temp_item;
    if (q->size == 1) {
        temp_item = q->first->value;

        free(q->first);
        q->first = NULL;
        q->last = NULL;
        q->size = 0;
    } else {
        temp_item = q->first->value;
        node* temp_node = q->first;
        
        q->first = temp_node->next;
        q->size--;
        free(temp_node);
    }

    return temp_item;


    error:
        return NULL;

}

//function to return the size of the queue
int size(queue* q) {
     return q->size;
}

//function to delete the queue
void delete_queue(queue *q) {
    FREE(q);
    return;
}