#include "join.h"

void *join_job(void *args) {

    //Make this shit work
    join_arguments *argm = (join_arguments *) args;
    relation *relR = argm->rel_R;
    relation *relS = argm->rel_S;

    uint32_t count = 0;
    for (ssize_t i = argm->start ; i < argm->end ; i++) {
        queue_node *qnode_R = (queue_node *) DArray_get(argm->queue_R, argm->indices_to_check[0][i]);
        queue_node *qnode_S = (queue_node *) DArray_get(argm->queue_S, argm->indices_to_check[1][i]);
        
        size_t pr = qnode_R->base;
        size_t r_end = qnode_R->base + qnode_R->size;
        size_t s_start = qnode_S->base;
        size_t s_end = qnode_S->base + qnode_S->size;

        while (pr < r_end && s_start < s_end) {
            
            size_t ps = s_start;
            int flag = 0;

            while ( ps < s_end) {
                if (relR->tuples[pr].key < relS->tuples[ps].key)
                    break;
                
                if (relR->tuples[pr].key > relS->tuples[ps].key){
                    ps++;
                    if (flag == 0) {
                        s_start = ps;
                    }
                }
                else {
                    count++;
                    flag = 1;
                    ps++;
                }
                
            }
            pr++;
        }
    }
    argm->found = count;

    return NULL;
}

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, thr_pool_t *pool, DArray *queue_S, DArray *queue_R) {
    
}