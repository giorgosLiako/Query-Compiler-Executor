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

    debug("%d ",is_sorted(temp_rel));

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

static DArray** mid_results_join(DArray *relation_infos, uint64_t *values_of_rel, uint64_t num_values, relation *rel, bool is_reversed) {

    DArray **row_ids = MALLOC(DArray *, DArray_count(relation_infos) + 1);

    for (ssize_t i = 0 ; i < DArray_count(relation_infos) + 1 ; i++) {
        row_ids[i] = DArray_create(sizeof(uint64_t), 100);
    }

    int mark = -1;
    size_t pr = 0;
    size_t ps = 0;

    if (!is_reversed) {
        while (pr < num_values && ps < rel->num_tuples) {
            
            if (mark == -1) {
                //while the key that points pr on R relation is smaller than the key that points the ps on S relation
                //increase the pointer pr 
                while (values_of_rel[pr] < rel->tuples[ps].key) {
                    pr++;
                }
                //while the key that points pr on R relation is bigger than the key that points the ps on S relation
                //increase the pointer ps 
                while (values_of_rel[pr]  > rel->tuples[ps].key) {
                    ps++; 
                }
                mark = ps; //mark this position of ps
            }
            if (values_of_rel[pr]  == rel->tuples[ps].key) {
                uint64_t payload = rel->tuples[ps].payload;

                for (ssize_t i = 0 ; i < DArray_count(relation_infos) ; i++) {
                    relation_info *rel_info = DArray_get(relation_infos, i);
                    DArray_push(row_ids[i], &rel_info->payloads[pr]);
                }
                DArray_push(row_ids[DArray_count(relation_infos)], &payload);
                ps++;
        
            } else { //the keys of the relations are different
                pr++;
                if (pr >= num_values) {
                    break;
                }
                if (values_of_rel[pr]  == rel->tuples[ps - 1].key) {
                    ps = (size_t) mark;
                }
                mark = -1; //initialize mark
            }
        }
    }

    return row_ids;
} 

static join_result classic_join(relation *relR, relation *relS, DArray *rel_infos) {

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

static int build_relations(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results, relation *rel[]) {

    uint64_t lhs_rel = relations[pred->first.relation]; 
    uint64_t lhs_col = pred->first.column; 

    relation_column *second = (relation_column *) pred->second;
    uint64_t rhs_rel = relations[second->relation]; 
    uint64_t rhs_col = second->column; 

    if (lhs_rel == rhs_rel && lhs_col == rhs_col) {
        return DO_NOTHING;
    }

    int error = 0;

    relation_info *lhs_info = relation_exists(mid_results, lhs_rel);
    relation_info *rhs_info = relation_exists(mid_results, rhs_rel);

    if (lhs_info == NULL && rhs_info == NULL) {
        mid_result *res = create_entity(mid_results, relations, pred);
        relation_info *lhs_info = add_to_entity(res, relations[pred->first.relation]);
        relation_info *rhs_info = add_to_entity(res, relations[second->relation]);
        
        error += allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);
        error += allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);

        return CLASSIC_JOIN;
    }
    else if (lhs_info != NULL && rhs_info != NULL) {
        error += allocate_relation_mid_results(rel, metadata_arr, lhs_info, lhs_rel, lhs_col, 0);
        error += allocate_relation_mid_results(rel, metadata_arr, rhs_info, rhs_rel, rhs_col, 1);

        return SCAN_JOIN;
    }
    else if (lhs_info != NULL && rhs_info == NULL) {
        error += allocate_relation(rel, metadata_arr, rhs_rel, rhs_col, 1);
        if (lhs_info->last_col_sorted == (int32_t) lhs_col) {
            return JOIN_SORT_RHS;
        } else {
            return JOIN_SORT_LHS_EXISTS;
        }
    }
    else if (lhs_info == NULL && rhs_info != NULL) {
        error += allocate_relation(rel, metadata_arr, lhs_rel, lhs_col, 0);
        if (rhs_info->last_col_sorted == (int32_t) rhs_col) {
            return JOIN_SORT_LHS;
        } else {
            return JOIN_SORT_RHS_EXISTS;
        }
    }

    return 0;

    error:
        return -1;
}

int cmp_values(const void *a, const void *b) {

    return ((uint64_t *) a) - ((uint64_t *) b);
}

void update_relations_info() {

    if (join_id == CLASSIC_JOIN) {
        relation_info *lhs_info  = (relation_info *) relation_exists(mid_results, lhs_relation);
        relation_info *rhs_info  = (relation_info *) relation_exists(mid_results, rhs_relation);   
    }
}

int execute_join(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results) {

    relation *rel[2];
    int retval = build_relations(pred, relations, metadata_arr, mid_results, rel);

    int error = 0;

    join_result res;

    if (retval == CLASSIC_JOIN) {
        iterative_sort(rel[0]);
        iterative_sort(rel[1]);

        res = call_join(rel, &error);
    }
    else if (retval == JOIN_SORT_LHS_EXISTS || retval == JOIN_SORT_RHS) {
        relation_info *rel_info = relation_exists(mid_results, relations[pred->first.relation]);
        if (rel_info == NULL) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        iterative_sort(rel[1]);
        uint64_t *values_of_rel, num_values;

        get_values(metadata_arr, rel_info, &values_of_rel, &num_values); 
        if (retval == JOIN_SORT_LHS_EXISTS) {
            qsort(values_of_rel, num_values, sizeof(uint64_t), cmp_values);
        }
        
        mid_results_join((DArray *) DArray_last(mid_results), values_of_rel, num_values, rel[1], 0);
    }
    else if (retval == JOIN_SORT_RHS_EXISTS || retval == JOIN_SORT_LHS) {
        relation_column *second = (relation_column *) pred->second;
        relation_info *rel_info = relation_exists(mid_results, relations[second->relation]);
        if (rel_info == NULL) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        iterative_sort(rel[0]);
      
        uint64_t *values_of_rel, num_values;
        get_values(metadata_arr, rel_info, &values_of_rel, &num_values);

        if (retval == JOIN_SORT_RHS_EXISTS) {
            qsort(values_of_rel, num_values, sizeof(uint64_t), cmp_values);
        }

        mid_results_join((DArray *) DArray_last(mid_results), values_of_rel, num_values, rel[0], 0);
    }
    else if (retval == SCAN_JOIN) {
        res = scan_join(rel[0], rel[1], &error);
    }

    update_mid_results();

    FREE(rel[0]->tuples);
    FREE(rel[0]);
    FREE(rel[1]->tuples);
    FREE(rel[1]);

    return error;
}

    rowid0 - 1, 6, 2, 2
    rowid1 - 0, 1, 3, 5
    rowid2 - 1, 5, 1, 5