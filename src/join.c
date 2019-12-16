#include "join.h"

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


bool is_sorted(relation *rel) {

    for (ssize_t i = 0 ; i < rel->num_tuples - 1; i++) {
        if (rel->tuples[i + 1].key < rel->tuples[i].key) {
            return false;
        }
    }

    return true;
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

    //debug("%d ",is_sorted(temp_rel));

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

static int build_relations(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results, relation *rel[]) {


    uint64_t lhs_rel = relations[pred->first.relation];
    uint64_t lhs_col = pred->first.column;

    relation_column *second = (relation_column *) pred->second;
    uint64_t rhs_rel = relations[second->relation];
    uint64_t rhs_col = second->column;

    if (lhs_rel == rhs_rel && lhs_col == rhs_col) {
        return DO_NOTHING;
    }

    ssize_t lhs_index = relation_exists(mid_results, lhs_rel);
    ssize_t rhs_index = relation_exists(mid_results, rhs_rel);
    
    /*Check if relations exist in the mid results and if they are sorted
    and create them accordingly*/
    int error = 0;
    
    if (lhs_index != -1 && rhs_index == -1) {
        error = allocate_relation_mid_results(rel, mid_results, metadata_arr, lhs_index, lhs_rel, lhs_col, 0);
        error = allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);

        check(error != -1, "Couldn't allocate relations");

        mid_result *mid_res = (mid_result *) DArray_get(mid_results, lhs_index);
        if (mid_res->last_column_sorted == (int32_t) lhs_col) {
            //debug("Only right relation needs sorting (%lu , %lu)", lhs_rel, rhs_rel);
            return JOIN_SORT_RHS;
        } else {
            //debug("Both relations need sorting (%lu , %lu)", lhs_rel, rhs_rel);
            mid_res->last_column_sorted = lhs_col;
            return CLASSIC_JOIN;
        }
    }
    else if (lhs_index != -1 && rhs_index != -1) {
        error = allocate_relation_mid_results(rel, mid_results, metadata_arr, lhs_index, lhs_rel, lhs_col, 0);
        error = allocate_relation_mid_results(rel, mid_results, metadata_arr, rhs_index, rhs_rel, rhs_col, 1);

        check(error != -1, "Couldn't allocate relations");

        return SCAN_JOIN;
    }
    else if (lhs_index == -1 && rhs_index != -1) {
        error = allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);
        error = allocate_relation_mid_results(rel, mid_results, metadata_arr, rhs_index, rhs_rel, rhs_col, 1);

        check(error != -1, "Couldn't allocate relations");

        mid_result *mid_res = (mid_result *) DArray_get(mid_results, rhs_index);
        if (mid_res->last_column_sorted == (int32_t) rhs_col) {
            return JOIN_SORT_LHS;
        } else {
            mid_res->last_column_sorted = rhs_col;
            return CLASSIC_JOIN;
        }
    }
    else if (lhs_index == -1 && rhs_index == -1) {
        error = allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);
        error = allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);

        check(error != -1, "Couldn't allocate relations");

        if (rhs_rel != lhs_rel) {
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

static join_result join_relations(relation *relR, relation *relS, int *error) {

    int mark = -1;  //mark for duplicates
    size_t pr = 0; 
    size_t ps = 0; 
    join_result res;

    DArray *results_r = DArray_create(sizeof(uint64_t), 10);
    check(results_r != NULL, "Couldnt allocate dynamic array");
    DArray *results_s = DArray_create(sizeof(uint64_t), 10);
    check(results_s != NULL, "Couldn't allocate dynamic array");

    uint32_t results = 0;

    while (pr < relR->num_tuples && ps < relS->num_tuples) {

        if (mark == -1) {
            //while the key that points pr on R relation is smaller than the key that points the ps on S relation
            //increase the pointer pr 
            while (relR->tuples[pr].key < relS->tuples[ps].key) {
                pr++;
            }
            //while the key that points pr on R relation is bigger than the key that points the ps on S relation
            //increase the pointer ps 
            while (relR->tuples[pr].key > relS->tuples[ps].key) {
                ps++; 
            }
            mark = ps; //mark this position of ps
        }

        if (relR->tuples[pr].key == relS->tuples[ps].key) {
            //count the join and add it to the result list    
            check(DArray_push(results_r, &(relR->tuples[pr].payload)) == 0, "Failed to push to array");
            check(DArray_push(results_s, &(relS->tuples[ps].payload)) == 0, "Failed to push to array");
            ps++;
        
        } else { //the keys of the relations are different
            pr++;
            if (pr >= relR->num_tuples) {
                break;
            }
            if (relR->tuples[pr].key == relS->tuples[ps - 1].key) {
                ps = (size_t) mark;
            }
            mark = -1; //initialize mark
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

static join_result scan_join(relation *relR, relation *relS, int *error) {

    ssize_t pr = 0, ps = 0;
    join_result res;

    DArray *results_r = DArray_create(sizeof(uint64_t), 10);
    check(results_r != NULL, "Couldnt allocate dynamic array");
    DArray *results_s = DArray_create(sizeof(uint64_t), 10);
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

static join_result call_join(relation *rel[], int *error) {

    if (rel[0]->num_tuples < rel[1]->num_tuples) {
        return join_relations(rel[0], rel[1], error);
    } else {
        return join_relations(rel[1], rel[0], error);
    }
}
static join_result remove_duplicates(join_result join_res) {
    DArray *payloads_R = join_res.results[0];
    DArray *payloads_S = join_res.results[1];

    DArray *results_r = DArray_create(sizeof(uint64_t), 10);
    check(results_r != NULL, "Couldnt allocate dynamic array");
    DArray *results_s = DArray_create(sizeof(uint64_t), 10);
    check(results_s != NULL, "Couldn't allocate dynamic array");


    join_result new_res;

    size_t size = DArray_count(payloads_R);
    size_t new_size = DArray_count(results_r);

    for (size_t i = 0; i < size; i++) {
        char found = 0;
        uint64_t R = *((uint64_t*) DArray_get(payloads_R,i));
        uint64_t S = *((uint64_t*) DArray_get(payloads_S,i));
        for (size_t j = 0; j < new_size; j++) {
            if (R == *((uint64_t*) DArray_get(results_r, j)) &&
                S == *((uint64_t*) DArray_get(results_s, j)) ){
                    found = 1;
                    break;
                }
        }

        if (found == 0) {   
            DArray_push(results_r, &R);
            DArray_push(results_s, &S);
            new_size++;
        }
    }
    new_res.results[0] = results_r;
    new_res.results[1] = results_s;

    error:
    return new_res;

}

DArray *new(join_result driver, DArray *last, DArray *edit, uint32_t mode){

    DArray *n = DArray_create(sizeof(uint64_t), 10);

    size_t count = DArray_count(n);
    size_t index = 0;
    // if (mode == 1) index = 0;
    // else index = 1;

    printf("non duplicate %u\n", DArray_count(driver.results[index]));
    printf("%u %u\n", DArray_count(last),  DArray_count(edit) );

    for (size_t i = 0; i < DArray_count(driver.results[index]); i++) {
        uint64_t id = *((uint64_t*) DArray_get(driver.results[index], i));
        for (size_t j = 0; j < DArray_count(last); j++) {
            if (id == *((uint64_t*) DArray_get(last, j))){
                count++;
                uint64_t n_id = *((uint64_t*) DArray_get(edit, j));
                DArray_push(n, &n_id);
            }
        }
    }

    

    return n;
}

static void update_mid_results(join_result join_res, DArray *mid_results, uint32_t relR, uint32_t colR, uint32_t relS, uint32_t colS, int join_id, uint32_t mode) {

    DArray *payloads_R = join_res.results[0];
    DArray *payloads_S = join_res.results[1];

    //debug("relR = %u, colR = %u , relS = %u, colS = %u", relR, colR, relS, colS);

    if (join_id == CLASSIC_JOIN) {
        /*If we sorted both relations, add them both to mid results */
        mid_result tmp;
        tmp.last_column_sorted = colR;
        tmp.payloads = payloads_R;
        tmp.relation = relR;
        ssize_t index = relation_exists(mid_results, relR);
        if (index == -1) {
            DArray_push(mid_results, &tmp);
        } else {
            //debug("Set instead of push (%u)", DArray_count(tmp.payloads));           
            DArray_set(mid_results, index, &tmp);
        }

        tmp.last_column_sorted = colS;
        tmp.payloads = payloads_S;
        tmp.relation = relS;
        index = relation_exists(mid_results, relS);
        if (index == -1) {
            DArray_push(mid_results, &tmp);
        } else {
            //debug("Set instead of push");
            DArray_set(mid_results, index, &tmp);
        }
    }
    else if (join_id == JOIN_SORT_LHS) {
        /*If we sorted only left relation, it means the right one was already in the mid results */
        mid_result tmp;
        tmp.last_column_sorted = colR;
        tmp.payloads = payloads_R;
        tmp.relation = relR;
        ssize_t index = relation_exists(mid_results, relR);
        if (index == -1) {
            DArray_push(mid_results, &tmp);
        } else {
            DArray_set(mid_results, index, &tmp);
        }

        index = relation_exists(mid_results, relS);
        if (index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
        mid_result *update = (mid_result *) DArray_get(mid_results, index);
        update->payloads = payloads_S;
        //debug("updated payload = %u", DArray_count(update->payloads));
    }
    else if (join_id == JOIN_SORT_RHS) {
        
    
        mid_result tmp_S;
        tmp_S.last_column_sorted = colS;
        tmp_S.payloads = payloads_S;
        tmp_S.relation = relS;
        
        size_t index = relation_exists(mid_results, relS);
        if (index == -1) {
            DArray_push(mid_results, &tmp_S);
        } else {
            //debug("Set instead of push");index = relation_exists(mid_results, relR);
            join_result no_dup = remove_duplicates(join_res);
            mid_result *update = (mid_result *) DArray_get(mid_results, index);
            
            for (size_t i = 0; i < DArray_count(mid_results); i++) {
                mid_result *edit = (mid_result *) DArray_get(mid_results, i);
                if (edit->relation != relR && edit->relation != relS){
                    edit->payloads = new(no_dup, update->payloads, edit->payloads, mode);         
                }
            }
            DArray_set(mid_results, index, &tmp_S);
        }

        mid_result tmp_R;
        tmp_R.last_column_sorted = colR;
        tmp_R.payloads = payloads_R;
        tmp_R.relation = relR;

        index = relation_exists(mid_results, relR);
        if (index == -1) {
            DArray_push(mid_results, &tmp_R);
        } else {
            join_result no_dup = remove_duplicates(join_res);
            mid_result *update = (mid_result *) DArray_get(mid_results, index);
            
            for (size_t i = 0; i < DArray_count(mid_results); i++) {
                mid_result *edit = (mid_result *) DArray_get(mid_results, i);
                if (edit->relation != relR && edit->relation != relS){
                    edit->payloads = new(no_dup, update->payloads, edit->payloads, mode);         
                }
            }
            DArray_set(mid_results, index, &tmp_R);
        }


    }
    else if (join_id == SCAN_JOIN) {
        ssize_t index = relation_exists(mid_results, relR);
        if (index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
        mid_result *update = (mid_result *) DArray_get(mid_results, index);
        update->payloads = payloads_R;

        index = relation_exists(mid_results, relS);
        if (index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
        update = (mid_result *) DArray_get(mid_results, index);
        update->payloads = payloads_S;
    }
}

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results) {

    relation *rel[2];
    int retval = build_relations(pred, relations, metadata_arr, mid_results, rel);

    int error = 0;

    join_result join_res;

    if (retval == CLASSIC_JOIN) {
        //Means its a classic join, sort both relations and join them
        debug("CLASSIC JOIN");
        iterative_sort(rel[0]);
        iterative_sort(rel[1]);

        join_res = call_join(rel, &error);
    }
    else if (retval == JOIN_SORT_LHS) {
        //Means one of the relations is already sorted, sorted only the left one
        debug("JOIN_SORT_LHS");
        iterative_sort(rel[0]);

        join_res = call_join(rel, &error);
    }
    else if (retval == JOIN_SORT_RHS) {
        debug("JOIN_SORT_RHS");
        iterative_sort(rel[1]);

        join_res = call_join(rel, &error);
    }
    else if (retval == SCAN_JOIN) {
        debug("SCAN_JOIN");
        join_res = scan_join(rel[0], rel[1], &error);
    }
    else if (retval == DO_NOTHING) {
        return 0;
    }
    else {
        return -1;
    }

    uint32_t mode = 1;

    if (rel[0]->num_tuples >= rel[1]->num_tuples && retval != SCAN_JOIN) {
        DArray *tmp = join_res.results[0];
        join_res.results[0] = join_res.results[1];
        join_res.results[1] = tmp;  
        mode = 0;
    }
    update_mid_results(join_res, mid_results, relations[pred->first.relation], pred->first.column, relations[((relation_column *) pred->second)->relation], ((relation_column *) pred->second)->column, retval, mode);


    FREE(rel[0]->tuples);
    FREE(rel[0]);
    FREE(rel[1]->tuples);
    FREE(rel[1]);

    return error;
}
