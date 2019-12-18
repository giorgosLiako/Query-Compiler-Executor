#include "join.h"
#include "hashmap.h"

//functions that sorts iterative one relation
static int iterative_sort(relation *rel) {
    if ( rel->num_tuples * sizeof(tuple) + sizeof(uint64_t) < 64*1024) {
        random_quicksort(rel->tuples, 0, rel->num_tuples - 1);
        return 0;
    }
    
    relation *reordered = allocate_reordered_array(rel);
    queue *q = new_queue();
    check_mem(q);

    relation *relations[2]; //to swap after each iteration

    histogram new_h, new_p;
    //build the first histogram and the first psum ( of all the array)
    build_histogram(rel, &new_h, 1, 0, rel->num_tuples);
    build_psum(&new_h, &new_p);
    //build the R' (reordered R)
    reordered = build_reordered_array(reordered, rel, &new_h, &new_p, 1, 0, rel->num_tuples);
    
    relations[0] = rel;
    relations[1] = reordered;

    //the byte take the values [0-255] , each value is a bucket
    //each bucket that found push it in the queue
    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0)
            push(q, new_item(new_p.array[j], new_h.array[j]));
                 
    }

    ssize_t i;
    int count = 1;
    //above we execute the routine for the first byte
    //now for all the other bytes
    for (i = 2; i < 9; i++) { //for each byte of 2 to 8
        
        int number_of_buckets = size(q);

        if (number_of_buckets > 0) {
            count++;
        } else {
            break;
        }
        //the size of the queue is the number of the buckets
        while (number_of_buckets) { //for each bucket
    
            item* current_item = pop(q);
            ssize_t base = current_item->base, size = current_item->size; // base = start of bucket , size = the number of cells of the bucket
            delete_item(current_item);
            number_of_buckets--;
            //check if the bucket is smaller than 64 KB to execute quicksort
            if ( size*sizeof(tuple) + sizeof(uint64_t) < 64*1024) {
                random_quicksort(relations[(i+1)%2]->tuples, base, base +size-1);
                for (ssize_t j = base; j < base + size; j++) {

                    relations[i%2]->tuples[j] = relations[(i+1)%2]->tuples[j];
                }

            } else { // if the bucket is bigger than 64 KB , sort by the next byte
                histogram new_h, new_p;
                //build again histogram and psum of the next byte
                build_histogram(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                //build the reordered array of the previous bucket
                relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                //push the buckets to the queue
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0)
                        push(q, new_item(base + new_p.array[j], new_h.array[j]));
                }
            }
        }
    }   
    if (count % 2 == 1) {
        for (size_t i = 0; i < rel->num_tuples; i++){
             rel->tuples[i] = relations[1]->tuples[i];
        }
        
    }
    //free allocated memory
    free_reordered_array(reordered);
    delete_queue(q);
    return 0;

    error:
        free_reordered_array(reordered);
        delete_queue(q);
        return -1;
}

static int allocate_relation_mid_results(relation *rel_arr[], DArray *mid_results, DArray *metadata_arr, ssize_t index, uint64_t rel, uint64_t col, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);
    check_mem(temp_rel);

    mid_result mid_res = *(mid_result *) DArray_get(mid_results, index);

    temp_rel->num_tuples = DArray_count(mid_res.payloads);
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);
    check_mem(temp_rel->tuples);
    relation *tmp_data = ((metadata *) DArray_get(metadata_arr, rel))->data[col];
    for (ssize_t i = 0 ; i < DArray_count(mid_res.payloads) ; i++) {
        
        uint64_t payload = * (uint64_t *) DArray_get(mid_res.payloads, i);
        temp_rel->tuples[i].key = tmp_data->tuples[payload].key;
        temp_rel->tuples[i].payload = payload;
    }

    rel_arr[rel_arr_index] = temp_rel;

    return 0;

    error:
        return -1;
}

static int allocate_relation(relation *rel_arr[], DArray *metadata_arr, uint64_t rel, uint64_t col, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);
    check_mem(temp_rel);

    relation *tmp_data = ((metadata *) DArray_get(metadata_arr, rel))->data[col];
    temp_rel->num_tuples = tmp_data->num_tuples;
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);

    for (size_t i = 0 ; i < tmp_data->num_tuples ; i++) {
        temp_rel->tuples[i].key = tmp_data->tuples[i].key;
        temp_rel->tuples[i].payload = tmp_data->tuples[i].payload;
    }

    rel_arr[rel_arr_index] = temp_rel;

    return 0;

    error:
        return -1;
}


static void create_entity_mid_results(DArray *mid_results_array) {

    DArray *mid_results = DArray_create(sizeof(mid_result), 4);

    DArray_push(mid_results_array, &mid_results);
}

static int build_relations(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, relation *rel[]) {

    uint64_t lhs_rel = relations[pred->first.relation];
    uint64_t lhs_col = pred->first.column;

    relation_column *second = (relation_column *) pred->second;
    uint64_t rhs_rel = relations[second->relation];
    uint64_t rhs_col = second->column;

    if (lhs_rel == rhs_rel && lhs_col == rhs_col) {
        return DO_NOTHING;
    }

    DArray *mid_results = NULL;
    if (DArray_count(mid_results_array) == 0) {
        mid_results = DArray_create(sizeof(mid_result), 4);
        DArray_push(mid_results_array, &mid_results);
    } 
    else { 
        mid_results = *(DArray **) DArray_last(mid_results_array);
    }

    ssize_t lhs_index = relation_exists_current(mid_results, lhs_rel, pred->first.relation);
    ssize_t rhs_index = relation_exists_current(mid_results, rhs_rel, second->relation);
    
    /*Check if relations exist in the mid results and if they are sorted
    and create them accordingly*/
    int error = 0;
    
    if (lhs_index != -1 && rhs_index == -1) {

        error += allocate_relation_mid_results(rel, mid_results, metadata_arr, lhs_index, lhs_rel, lhs_col, 0);

        exists_info exists = relation_exists(mid_results_array, rhs_rel, second->relation);
        DArray *tmp_mid_results = NULL;
        if (exists.index == -1) {
            error += allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);
        } else {
            tmp_mid_results = * (DArray **) DArray_get(mid_results_array, exists.mid_result);
            error += allocate_relation_mid_results(rel, tmp_mid_results, metadata_arr, exists.index, rhs_rel, rhs_col, 1);
        }

        check(error == 0, "Couldn't allocate relations");

        mid_result *mid_res = (mid_result *) DArray_get(mid_results, lhs_index);
        if (tmp_mid_results == NULL) {
            if (mid_res->last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_RHS;
            } else {
                mid_res->last_column_sorted = lhs_col;
                return CLASSIC_JOIN;
            }
        }
        else {
            mid_result *tmp_mid_result = (mid_result *) DArray_get(tmp_mid_results, exists.index);
            if (mid_res->last_column_sorted == (int32_t) lhs_col && tmp_mid_result->last_column_sorted == (int32_t) rhs_col) {
                return SCAN_JOIN;
            }
            else if (mid_res->last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_RHS;
            }
            else if (tmp_mid_result->last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_LHS;
            }
            else {
                return CLASSIC_JOIN;
            }
        }
    }
    else if (lhs_index != -1 && rhs_index != -1) {
        error += allocate_relation_mid_results(rel, mid_results, metadata_arr, lhs_index, lhs_rel, lhs_col, 0);
        error += allocate_relation_mid_results(rel, mid_results, metadata_arr, rhs_index, rhs_rel, rhs_col, 1);

        check(error == 0, "Couldn't allocate relations");

       return SCAN_JOIN;
    }
    else if (lhs_index == -1 && rhs_index != -1) {

        error += allocate_relation_mid_results(rel, mid_results, metadata_arr, rhs_index, rhs_rel, rhs_col, 1);

        exists_info exists = relation_exists(mid_results_array, lhs_rel, pred->first.relation);
        DArray *tmp_mid_results = NULL;
        if (exists.index == -1) {
            error += allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);
        } else {
            tmp_mid_results = * (DArray **) DArray_get(mid_results_array, exists.mid_result);
            error += allocate_relation_mid_results(rel, tmp_mid_results, metadata_arr, exists.index, lhs_rel, lhs_col, 0);
        }

        check(error == 0, "Couldn't allocate relations");
        
        mid_result *mid_res = (mid_result *) DArray_get(mid_results, rhs_index);
        if (tmp_mid_results == NULL) {
            if (mid_res->last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_LHS;
            } else {
                mid_res->last_column_sorted = rhs_col;
                return CLASSIC_JOIN;
            }
        }
        else {
            mid_result *tmp_mid_result = (mid_result *) DArray_get(tmp_mid_results, exists.index);
            if (mid_res->last_column_sorted == (int32_t) rhs_col && tmp_mid_result->last_column_sorted == (int32_t) lhs_col) {
                return SCAN_JOIN;
            }
            else if (mid_res->last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_RHS;
            }
            else if (mid_res->last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_LHS;
            }
            else {
                return CLASSIC_JOIN;
            }
        }
        
    }
    else if (lhs_index == -1 && rhs_index == -1) {

        create_entity_mid_results(mid_results_array);

        error += allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);
        error += allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);

        check(error == 0, "Couldn't allocate relations");

        if (rhs_rel != lhs_rel || pred->first.relation != second->relation) {
            return CLASSIC_JOIN;
        }
        else {
            return SCAN_JOIN;
        }
    }

    return 0;

    error:
        return -1;

}

typedef struct payloads {
    uint64_t payload_R;
    uint64_t payload_S;
} payloads;


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

static join_result join_relations(relation *relR, relation *relS, int *error) {
    
    size_t pr = 0; 
    size_t s_start = 0; 
    join_result res;
    res.results[0] = NULL;
    res.results[1] = NULL;

    DArray *results_r = DArray_create(sizeof(uint64_t), 1000);
    check(results_r != NULL, "Couldn't allocate dynamic array");
    DArray *results_s = DArray_create(sizeof(uint64_t), 1000);
    check(results_s != NULL, "Couldn't allocate dynamic array");
    DArray *non_duplicates_r = DArray_create(sizeof(uint64_t), 1000);
    DArray *non_duplicates_s = DArray_create(sizeof(uint64_t), 1000);

    Hashmap *map = Hashmap_create(sizeof(payloads), sizeof(payloads), hashmap_compare, hashmap_hash);

    while (pr < relR->num_tuples && s_start < relS->num_tuples) {
        
        size_t ps = s_start;
        int flag = 0;

        while ( ps < relS->num_tuples){
            if (relR->tuples[pr].key < relS->tuples[ps].key)
                break;
            
            if (relR->tuples[pr].key > relS->tuples[ps].key){
                ps++;
                if (flag == 0) {
                    s_start = ps;
                }
            }
            else {
                payloads pay_l;
                pay_l.payload_R = relR->tuples[pr].payload;
                pay_l.payload_S = relS->tuples[ps].payload;

                if (Hashmap_get(map, &pay_l) == NULL) {
                    //If you didn't find that entry before, push it
                    check(DArray_push(non_duplicates_r, &(relR->tuples[pr].payload)) == 0, "Failed to push to array");
                    check(DArray_push(non_duplicates_s, &(relS->tuples[ps].payload)) == 0, "Failed to push to array");
                    Hashmap_set(map, &pay_l, &pay_l);
                }
                check(DArray_push(results_r, &(relR->tuples[pr].payload)) == 0, "Failed to push to array");
                check(DArray_push(results_s, &(relS->tuples[ps].payload)) == 0, "Failed to push to array");
                flag = 1;
                ps++;
            }
            
        }
        pr++;
        debug("pr = %u", pr);
    }

    Hashmap_destroy(map);

    res.results[0] = results_r;
    res.results[1] = results_s;
    res.non_duplicates[0] = non_duplicates_r;
    res.non_duplicates[1] = non_duplicates_s;

    *error = 0;
    return res;

    error:
        *error = 1;
        return res;
} 


static join_result scan_join(relation *relR, relation *relS, int *error) {

    ssize_t pr = 0, ps = 0;
    join_result res;

    DArray *results_r = DArray_create(sizeof(uint64_t), 100);
    check(results_r != NULL, "Couldnt allocate dynamic array");
    DArray *results_s = DArray_create(sizeof(uint64_t), 100);
    check(results_s != NULL, "Couldn't allocate dynamic array");

    uint64_t iterations = relR->num_tuples < relS->num_tuples ? relR->num_tuples : relS->num_tuples;

    for (size_t i = 0 ; i < iterations ; i++ , pr++, ps++) {
        if (relR->tuples[pr].key == relS->tuples[ps].key) {
            check(DArray_push(results_r, &(relR->tuples[pr].payload)) == 0, "Failed to push to array");
            check(DArray_push(results_s, &(relS->tuples[ps].payload)) == 0, "Failed to push to array");
        }
    }

    res.results[0] = results_r;
    res.results[1] = results_s;

    *error = 0;
    return res;

    error:
        *error = 1;
        return res;
} 


static DArray* join_payloads(DArray *driver, DArray *last, DArray *edit) {

    relation *relR = MALLOC(relation, 1);
    relR->num_tuples = DArray_count(last);
    relR->tuples = MALLOC(tuple, relR->num_tuples);
    for (size_t i = 0 ; i < relR->num_tuples ; i++) {
        relR->tuples[i].key = * (uint64_t *) DArray_get(last, i);
        relR->tuples[i].payload = * (uint64_t *) DArray_get(edit, i);
    }

    relation *relS = MALLOC(relation, 1);
    relS->num_tuples = DArray_count(driver);
    relS->tuples = MALLOC(tuple, relS->num_tuples);
    for (size_t i = 0 ; i < relS->num_tuples ; i++) {
        relS->tuples[i].key = * (uint64_t *) DArray_get(driver, i);
        relS->tuples[i].payload = 0;
    }

    iterative_sort(relR);
    iterative_sort(relS);

    size_t pr = 0; 
    size_t s_start = 0; 

    DArray *result = DArray_create(sizeof(uint64_t), 1000);

    while (pr < relR->num_tuples && s_start < relS->num_tuples) {
        
        size_t ps = s_start;
        int flag = 0;

        while ( ps < relS->num_tuples){
            if (relR->tuples[pr].key < relS->tuples[ps].key)
                break;
            
            if (relR->tuples[pr].key > relS->tuples[ps].key){
                ps++;
                if (flag == 0) {
                    s_start = ps;
                }
            }
            else {
                DArray_push(result, &relR->tuples[pr].payload);
                flag = 1;
                ps++;
            }
            
        }
        pr++;
        debug("pr = %u", pr);
    }

    FREE(relR->tuples);
    FREE(relR);
    FREE(relS->tuples);
    FREE(relS);

    return result;
}

static void fix_all_mid_results(join_result join_res, exists_info exists, DArray *mid_results_array, uint32_t relR, uint32_t relS, mid_result tmp, int mode) {
        
        DArray *no_dup = join_res.non_duplicates[mode];

        DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        mid_result *update = (mid_result *) DArray_get(mid_results, exists.index);

        for (size_t i = 0; i < DArray_count(mid_results); i++) {
            mid_result *edit = (mid_result *) DArray_get(mid_results, i);
            if (edit->relation != relR && edit->relation != relS) {
                edit->payloads = join_payloads(no_dup, update->payloads, edit->payloads);   
            }
        }
        mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
        DArray_destroy(destroy->payloads);
        DArray_set(mid_results, exists.index, &tmp);
}

static void update_mid_results(join_result join_res, DArray *mid_results_array, uint64_t relR , uint64_t predR_id , uint64_t colR, uint64_t relS , uint64_t predS_id, uint64_t colS, int join_id) {
    
    DArray *payloads_R = join_res.results[0];
    DArray *payloads_S = join_res.results[1];

    if (join_id == CLASSIC_JOIN) {
        /*If we sorted both relations, add them both to mid results */
        mid_result tmp_R;
        tmp_R.last_column_sorted = colR;
        tmp_R.payloads = payloads_R;
        tmp_R.relation = relR;
        tmp_R.predicate_id = predR_id;

        exists_info exists = relation_exists(mid_results_array, relR , predR_id);
        if (exists.index == -1) {
            DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
            DArray_push(mid_results, &tmp_R);
        } else {
            fix_all_mid_results(join_res, exists, mid_results_array, relR, relS, tmp_R, 0);
        }

        mid_result tmp_S;
        tmp_S.last_column_sorted = colS;
        tmp_S.payloads = payloads_S;
        tmp_S.relation = relS;
        tmp_S.predicate_id = predS_id;

        exists = relation_exists(mid_results_array, relS, predS_id);
        if (exists.index == -1) {
            DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
            DArray_push(mid_results, &tmp_S);
            debug("pushed to array");
        } else {
            fix_all_mid_results(join_res, exists, mid_results_array, relR, relS, tmp_S, 1);
        }
    }
    else if (join_id == JOIN_SORT_LHS) {
        /*If we sorted only left relation, it means the right one was already in the mid results */
        mid_result tmp_R;
        tmp_R.last_column_sorted = colR;
        tmp_R.payloads = payloads_R;
        tmp_R.relation = relR;
        tmp_R.predicate_id = predR_id;

        exists_info exists = relation_exists(mid_results_array, relR , predR_id);
        if (exists.index == -1) {
            DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
            DArray_push(mid_results, &tmp_R);
        } else {
            DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
            mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
            DArray_destroy(destroy->payloads);
            DArray_set(mid_results, exists.index, &tmp_R);
        }

        exists = relation_exists(mid_results_array, relS , predS_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        mid_result tmp_S;
        tmp_S.last_column_sorted = colS;
        tmp_S.payloads = payloads_S;
        tmp_S.relation = relS;
        tmp_S.predicate_id = predS_id;

        fix_all_mid_results(join_res, exists, mid_results_array, relR, relS, tmp_S, 1);
    }
    else if (join_id == JOIN_SORT_RHS) {
        mid_result tmp_S;
        tmp_S.last_column_sorted = colS;
        tmp_S.payloads = payloads_S;
        tmp_S.relation = relS;
        tmp_S.predicate_id = predS_id;
        
        exists_info exists = relation_exists(mid_results_array, relS , predS_id);
        if (exists.index == -1) {
            DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
            DArray_push(mid_results, &tmp_S);
        } else {
            DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
            mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
            DArray_destroy(destroy->payloads);
            DArray_set(mid_results, exists.index, &tmp_S);
        }

        mid_result tmp_R;
        tmp_R.last_column_sorted = colR;
        tmp_R.payloads = payloads_R;
        tmp_R.relation = relR;
        tmp_R.predicate_id = predR_id;

        exists = relation_exists(mid_results_array, relR , predR_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        fix_all_mid_results(join_res, exists, mid_results_array, relR, relS, tmp_R, 0);
    }
    else if (join_id == SCAN_JOIN) {
        exists_info exists = relation_exists(mid_results_array, relR, predR_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
       
        DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        mid_result *update = (mid_result *) DArray_get(mid_results, exists.index);
        update->payloads = payloads_R;

        exists = relation_exists(mid_results_array, relS , predS_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
        
        mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        update = (mid_result *) DArray_get(mid_results, exists.index);
        update->payloads = payloads_S;
    }

    DArray_destroy(join_res.non_duplicates[0]);
    DArray_destroy(join_res.non_duplicates[1]);
}

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array) {

    relation *rel[2];

    int retval = build_relations(pred, relations, metadata_arr, mid_results_array, rel);
    int error = 0;

    join_result join_res;

    if (retval == CLASSIC_JOIN) {
        //Means its a classic join, sort both relations and join them
        iterative_sort(rel[0]);
        iterative_sort(rel[1]);

        join_res = join_relations(rel[0], rel[1], &error);
    }
    else if (retval == JOIN_SORT_LHS) {
        //Means one of the relations is already sorted, sorted only the left one
        iterative_sort(rel[0]);

        join_res = join_relations(rel[0], rel[1], &error);
    }
    else if (retval == JOIN_SORT_RHS) {
        iterative_sort(rel[1]);

        join_res = join_relations(rel[0], rel[1], &error);
    }
    else if (retval == SCAN_JOIN) {
        join_res = scan_join(rel[0], rel[1], &error);
    }
    else if (retval == DO_NOTHING) {
        return 0;
    }
    else {
        return -1;
    }

    update_mid_results(join_res, mid_results_array, relations[pred->first.relation],pred->first.relation, pred->first.column, relations[((relation_column *) pred->second)->relation],((relation_column *) pred->second)->relation , ((relation_column *) pred->second)->column, retval);

    FREE(rel[0]->tuples);
    FREE(rel[0]);
    FREE(rel[1]->tuples);
    FREE(rel[1]);

    return error;
}
