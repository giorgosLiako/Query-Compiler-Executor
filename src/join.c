#include "join.h"
#include "join_utilities.h"
#include "hashmap.h"

/*
(TODO) join enumeration
*/

join_result scan_join(DArray *relR, DArray *relS) {

    join_result res;

    DArray *results_r = DArray_create(sizeof(tuple), 2000);
    DArray *results_s = DArray_create(sizeof(tuple), 2000);

    uint32_t iterations = DArray_count(relR) < DArray_count(relS) ? DArray_count(relR) : DArray_count(relS);

    for (size_t i = 0 ; i < iterations ; i++) {
        tuple *tup_R = (tuple *) DArray_get(relR, i);
        tuple *tup_S = (tuple *) DArray_get(relS, i);
        if (tup_R->key == tup_S->key) {
            DArray_push(results_r, tup_R);
            DArray_push(results_s, tup_S);
        }
    }

    res.results[0] = results_r;
    res.results[1] = results_s;

    return res;

}

int32_t hashmap_compare(const void *lhs, const void *rhs) {

    const payloads pay_lhs = *(const payloads *) lhs;
    const payloads pay_rhs = *(const payloads *) rhs;

    if (pay_lhs.payload_R == pay_rhs.payload_R && pay_rhs.payload_S == pay_lhs.payload_S) {
        return 0;
    } 
    else {
        return 1;
    }
}

uint32_t hashmap_hash(void *key) {

    payloads pay_l = *(payloads *) key;
    uint32_t u_key = ((pay_l.payload_R & 0xFFFF) << 16) | (pay_l.payload_S & 0xFFFF);
    u_key = (u_key ^ 61) ^ (u_key >> 16);
    u_key = u_key + (u_key << 3);
    u_key = u_key ^ (u_key >> 4);
    u_key = u_key * 0x27d4eb2d;
    u_key = u_key ^ (u_key >> 15);
    return u_key;
}

join_result join_relations_single_threaded(DArray *relR, DArray *relS) {

    join_result join_res;

    DArray *results_r = DArray_create(sizeof(tuple), 2000);
    DArray *results_s = DArray_create(sizeof(tuple), 2000);
    DArray *no_duplicates_r = DArray_create(sizeof(uint64_t), 2000);
    DArray *no_duplicates_s = DArray_create(sizeof(uint64_t), 2000);

    size_t pr = 0; 
    size_t s_start = 0; 

    Hashmap *map = Hashmap_create(sizeof(payloads), sizeof(payloads), hashmap_compare, hashmap_hash);

    while (pr < DArray_count(relR) && s_start < DArray_count(relS)) {

        size_t ps = s_start;
        int flag = 0;
        while (ps < DArray_count(relS)) {
            tuple *tup_pr = (tuple *) DArray_get(relR, pr);
            tuple *tup_ps = (tuple *) DArray_get(relS, ps);
            if (tup_pr->key < tup_ps->key)
                break;
            
            if (tup_pr->key > tup_ps->key){
                ps++;
                if (flag == 0) {
                    s_start = ps;
                }
            }
            else {
                payloads pay_l;
                pay_l.payload_R = tup_pr->payload;
                pay_l.payload_S = tup_ps->payload;
                
                if (Hashmap_get(map, &pay_l) == NULL) {
                    //If we didn't push that entry before, push it now
                    DArray_push(no_duplicates_r, &tup_pr->payload);
                    DArray_push(no_duplicates_s, &tup_ps->payload);
                    Hashmap_set(map, &pay_l, &pay_l);
                }
                DArray_push(results_r, tup_pr);
                DArray_push(results_s, tup_ps);

                flag = 1;
                ps++;
            }  
        }
        pr++;
    }

    join_res.results[0] = results_r;
    join_res.results[1] = results_s;
    join_res.no_duplicates[0] = no_duplicates_r;
    join_res.no_duplicates[1] = no_duplicates_s;
    
    Hashmap_destroy(map);
    
    return join_res;
}

void *join_job(void *args) {

    join_arguments *argm = (join_arguments *) args;
    DArray *relR = argm->rel_R;
    DArray *relS = argm->rel_S;

    join_result join_res;

    DArray *results_r = DArray_create(sizeof(tuple), 200);
    check_mem(results_r);
    DArray *results_s = DArray_create(sizeof(tuple), 200);
    check_mem(results_s);
    DArray *no_duplicates_r = DArray_create(sizeof(uint64_t), 200);
    check_mem(no_duplicates_r); 
    DArray *no_duplicates_s = DArray_create(sizeof(uint64_t), 200);
    check_mem(no_duplicates_s);

    Hashmap *map = Hashmap_create(sizeof(payloads), sizeof(payloads), hashmap_compare, hashmap_hash);

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

            while (ps < s_end) {
                tuple *tup_pr = (tuple *) DArray_get(relR, pr);
                tuple *tup_ps = (tuple *) DArray_get(relS, ps);
                if (tup_pr->key < tup_ps->key)
                    break;
                
                if (tup_pr->key > tup_ps->key){
                    ps++;
                    if (flag == 0) {
                        s_start = ps;
                    }
                }
                else {
                    payloads pay_l;
                    pay_l.payload_R = tup_pr->payload;
                    pay_l.payload_S = tup_ps->payload;
                    
                    if (Hashmap_get(map, &pay_l) == NULL) {
                        //If we didn't push that entry before, push it now
                        check(DArray_push(no_duplicates_r, &tup_pr->payload) == 0, "Failed to push to array");
                        check(DArray_push(no_duplicates_s, &tup_ps->payload) == 0, "Failed to push to array");
                        Hashmap_set(map, &pay_l, &pay_l);
                    }
                    tuple to_push_r = *tup_pr;
                    tuple to_push_s = *tup_ps;
                    check(DArray_push(results_r, &to_push_r) == 0, "Failed to push to array");
                    check(DArray_push(results_s, &to_push_s) == 0, "Failed to push to array");

                    flag = 1;
                    ps++;
                }
                
            }
            pr++;
        }
    }

    join_res.results[0] = results_r;
    join_res.results[1] = results_s;
    join_res.no_duplicates[0] = no_duplicates_r;
    join_res.no_duplicates[1] = no_duplicates_s;

    argm->join_res = join_res;

    Hashmap_destroy(map);
    
    error:
        return NULL;
}


join_result join_relations(DArray *relR, DArray *relS, DArray *queue_R, DArray *queue_S, uint32_t jobs_to_create, thr_pool_t *pool) {

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

    join_arguments *params = MALLOC(join_arguments, jobs_to_create);
    for (ssize_t i = 0 ; i < jobs_to_create ; i++) {
        
        uint32_t start = i * (common_bytes / jobs_to_create);
        uint32_t end = (i + 1) * (common_bytes / jobs_to_create);
        if (i == jobs_to_create - 1) {
            end += common_bytes % jobs_to_create;
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

    join_result res;

    DArray *concat_r = params[0].join_res.results[0];
    DArray *concat_s = params[0].join_res.results[1];
    DArray *concat_no_dup_r = params[0].join_res.no_duplicates[0];
    DArray *concat_no_dup_s = params[0].join_res.no_duplicates[1];

    for (ssize_t i = 1 ; i < jobs_to_create ; i++) {
        /*Concatenate all results from jobs*/
        DArray *results_r = params[i].join_res.results[0];
        DArray *results_s = params[i].join_res.results[1];
        DArray *no_duplicates_r = params[i].join_res.no_duplicates[0];
        DArray *no_duplicates_s = params[i].join_res.no_duplicates[1];
        
        for (size_t j = 0 ; j < DArray_count(results_r) ; j++) {
            DArray_push(concat_r, DArray_get(results_r, j));
            DArray_push(concat_s, DArray_get(results_s, j));
        }
        for (size_t j = 0 ; j < DArray_count(no_duplicates_r) ; j++) {
            DArray_push(concat_no_dup_r, DArray_get(no_duplicates_r, j));
            DArray_push(concat_no_dup_s, DArray_get(no_duplicates_s, j));
        }

        DArray_destroy(results_r);
        DArray_destroy(results_s);
        DArray_destroy(no_duplicates_r);
        DArray_destroy(no_duplicates_s);
    }

    FREE(indices_to_check[0]);
    FREE(indices_to_check[1]);
    FREE(indices_to_check);
    FREE(params);

    res.results[0] = concat_r;
    res.results[1] = concat_s;
    res.no_duplicates[0] = concat_no_dup_r;
    res.no_duplicates[1] = concat_no_dup_s;
    
    return res;
}

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, thr_pool_t *pool) {

    rel_info *rel[2];
    rel[0] = MALLOC(rel_info, 1);
    rel[1] = MALLOC(rel_info, 1);

    /*Possible methods :
    1) {parallel queries}
    2) {parallel queries, sort (buckets)}
    3) {parallel queries, sort (buckets), join}
    4) {parallel queries, sort (2 sort parallel), join}
    5) {parallel sort (buckets), join}
*/

    int retval = sort_relations(pred, relations, metadata_arr, mid_results_array, rel, pool);
    int error = 0;

    join_result join_res;

    uint32_t first_relation = relations[pred->first.relation];
    uint32_t second_relation = relations[((relation_column *) pred->second)->relation];
    uint32_t first_column = pred->first.column;
    uint32_t second_column =  ((relation_column *) pred->second)->column;

    if (retval == SCAN_JOIN) {
        join_res = scan_join(rel[0]->rel, rel[1]->rel);
    }
    else if (retval == DO_NOTHING) {
        return 0;
    }
    else {
        uint32_t jobs_to_create = rel[0]->jobs_to_create < rel[1]->jobs_to_create ? rel[0]->jobs_to_create : rel[1]->jobs_to_create;

        if (jobs_to_create != 0) {
            join_res = join_relations(rel[0]->rel, rel[1]->rel, rel[0]->queue, rel[1]->queue, jobs_to_create, pool);
        }
        else {
            join_res = join_relations_single_threaded(rel[0]->rel, rel[1]->rel);
        }
    }

    join_info info;
    info.relR = first_relation;
    info.relS = second_relation;
    info.colR = first_column;
    info.colS = second_column;
    info.predR_id = pred->first.relation;
    info.predS_id = ((relation_column *) pred->second)->relation;
    info.join_id = retval;
    info.join_res = join_res;

    update_mid_results(mid_results_array, metadata_arr, info, rel);

    if (rel[0]->destroy_rel) {
        DArray_destroy(rel[0]->rel);
    }
    if (rel[1]->destroy_rel) {
        DArray_destroy(rel[1]->rel);
    }
    if (rel[0]->queue) {
        DArray_destroy(rel[0]->queue);
    }
    if (rel[1]->queue) {
        DArray_destroy(rel[1]->queue);
    }
    FREE(rel[0]);
    FREE(rel[1]);

    return error;
}