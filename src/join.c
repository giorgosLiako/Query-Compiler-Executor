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

int execute_join(relation *relR, relation *relS, DArray *queue_R, DArray *queue_S, thr_pool_t *pool) {

    /*(TODO): relR kai relS exoun ta aparaitita arguments, to spasimo twn buckets exei ginei hdh stin sort
    Na fitaksw tin join wste na kanei sort mono se ena plithos apo buckets me vash to queue(base, size);*/

    uint32_t common_bytes = 0;
    uint16_t **indices_to_check = MALLOC(uint16_t *, 2);
    indices_to_check[0] = MALLOC(uint16_t, 256);
    indices_to_check[1] = MALLOC(uint16_t, 256);

    for (ssize_t i = 0 ; i < DArray_count(queue_R) ; i++) {
        queue_node *qnode_R = (queue_node *) DArray_get(queue_R, i);
        for (ssize_t j = 0 ; j < DArray_count(queue_S) ; j++) {
            queue_node *qnode_S = (queue_node *) DArray_get(queue_S, j);
            if (qnode_S->byte == qnode_R->byte) {
                indices_to_check[0][common_bytes] = i; 
                indices_to_check[1][common_bytes] = j;
                common_bytes++;
            }
        }
    }   

    join_arguments *params = MALLOC(join_arguments, MAX_JOBS);
    for (ssize_t i = 0 ; i < MAX_JOBS ; i++) {
        
        uint32_t start = i * (common_bytes / MAX_JOBS);
        uint32_t end = (i + 1) * (common_bytes / MAX_JOBS);
        if (i == MAX_JOBS - 1) {
            end += common_bytes % MAX_JOBS;
        }
        params[i].end = end;
        params[i].indices_to_check = indices_to_check;
        params[i].start = start;
        params[i].rel_R = relR;
        params[i].rel_S = relS;
        params[i].queue_R = queue_R;
        params[i].queue_S = queue_S;
        
        thr_pool_queue(pool, join_job, (void *) &params[i]);
    }
    thr_pool_barrier(pool);

    uint32_t found_total = 0;
    for (ssize_t i = 0 ; i < MAX_JOBS ; i++) {
        found_total += params[i].found;
    }
    printf("found_total = %u\n", found_total);

    DArray_destroy(params[0].queue_R);
    DArray_destroy(params[0].queue_S);
    FREE(indices_to_check[0]);
    FREE(indices_to_check[1]);
    FREE(indices_to_check);
    
    return 0;
}