

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
