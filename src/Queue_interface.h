
#include "Queue.h"

queue* new_queue(void);
item* new_item(ssize_t , ssize_t );
void delete_item(item* );
void push(queue* , item* );
item* pop(queue *);
int size(queue* );
void delete_queue(queue *);