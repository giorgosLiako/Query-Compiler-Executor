#ifndef QUEUE_H
#define QUEUE_H

#include <stdio.h>

typedef struct item{
    ssize_t base;
    ssize_t size;
} item;


typedef struct node {
    item* value;
    struct node* next;
} node;


typedef struct queue{
    ssize_t size;
    node* first;
    node* last;
} queue;

queue* new_queue(void);

item* new_item(ssize_t , ssize_t );

void delete_item(item* );

void push(queue* , item* );

item* pop(queue *);

int size(queue* );

void delete_queue(queue *);

#endif
