#include "join_utilities.h"
#include "quicksort.h"
#include "stretchy_buffer.h"

void create_new_queue(relation *rel, queue_node **retval, uint32_t *jobs_created) {
	
    relation *reordered = allocate_reordered_array(rel);
    queue_node *q = NULL;

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
    queue_node *q_to_return = NULL;
    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0) {
            queue_node q_node;
            q_node.byte = j;
            q_node.base = new_p.array[j];
            q_node.size = new_h.array[j];
            buf_push(q, q_node);
        }
    }
    ssize_t i;
    int number_of_buckets = 0;
    //above we execute the routine for the first byte
    //now for all the other bytes
    for (i = 2; i <= 8 ; i++) {
        
        number_of_buckets = buf_len(q);
        if (number_of_buckets >= 2) {
            for (ssize_t tmp_i = 0 ; tmp_i < buf_len(q) ; tmp_i++) {
                queue_node q_node = q[tmp_i];
				buf_push(q_to_return, q_node);
            }
			break;
        }

        //the size of the queue is the number of the buckets
        while (number_of_buckets) { //for each bucket
    
            queue_node *current_item = &(q[0]);
            ssize_t base = current_item->base, size = current_item->size; // base = start of bucket , size = the number of cells of the bucket
            buf_remove(q, 0);
          
            number_of_buckets--;
            //check if the bucket is smaller than 32 KB to execute quicksort
            if ( size*sizeof(tuple) + sizeof(uint64_t) >= 32*1024) {
				// if the bucket is bigger than 32 KB , sort by the next byte
                histogram new_h, new_p;
                //build again histogram and psum of the next byte
                build_histogram(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                //build the reordered array of the previous bucket
                relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                //push the buckets to the queue
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0) {
                        queue_node q_node;
                        q_node.byte = j;
                        q_node.base = base + new_p.array[j];
                        q_node.size = new_h.array[j];
                        buf_push(q, q_node);
                    }
                }
			}
        }
    }
	number_of_buckets = buf_len(q);
    //If it wasn't yet sorted, try multithreading
    if ((number_of_buckets / MAX_JOBS) > 0) {
        
        uint32_t jobs_to_create;

        if (number_of_buckets / MAX_JOBS > 20) {
            jobs_to_create = MAX_JOBS;
        }
        else {
            jobs_to_create = 2;
        }
        *jobs_created = jobs_to_create;
    }
    else {
        *jobs_created = 0;
    }
	*retval = q_to_return;

    FREE(reordered->tuples);
    FREE(reordered);
	buf_free(q);
}

static void allocate_relation_mid_results(rel_info *rel_arr[], mid_result mid_res, tuple *tuples, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);

    temp_rel->num_tuples = buf_len(mid_res.payloads);
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);
    for (size_t i = 0 ; i < buf_len(mid_res.payloads) ; i++) {
        temp_rel->tuples[i].key = tuples[mid_res.payloads[i]].key;
        temp_rel->tuples[i].payload = i;
    }

    rel_arr[rel_arr_index]->tuples = temp_rel;
}

static void allocate_relation(rel_info *rel_arr[], relation *rel, ssize_t rel_arr_index) {

    relation *temp_rel = MALLOC(relation, 1);

    temp_rel->num_tuples = rel->num_tuples;
    temp_rel->tuples = MALLOC(tuple, temp_rel->num_tuples);
    for (size_t i = 0 ; i < temp_rel->num_tuples ; i++) {
        temp_rel->tuples[i].key = rel->tuples[i].key;
        temp_rel->tuples[i].payload = rel->tuples[i].payload;
    }

    rel_arr[rel_arr_index]->tuples = temp_rel;

}


int build_relations(predicate *pred, uint32_t *relations, metadata *metadata_arr, mid_result ***mid_results_array_ptr, rel_info *rel[]) {

    uint64_t lhs_rel = relations[pred->first.relation];
    uint64_t lhs_col = pred->first.column;

    relation_column *second = (relation_column *) pred->second;
    uint64_t rhs_rel = relations[second->relation];
    uint64_t rhs_col = second->column;

    if (lhs_rel == rhs_rel && lhs_col == rhs_col) {
        return DO_NOTHING;
    }

    mid_result **mid_results_array = *mid_results_array_ptr;

    mid_result *mid_results = NULL;
	if (buf_len(*mid_results_array) > 0) {
		mid_results = mid_results_array[buf_len(mid_results_array) - 1];
	}

    ssize_t lhs_index = relation_exists_current(mid_results, lhs_rel, pred->first.relation);
    ssize_t rhs_index = relation_exists_current(mid_results, rhs_rel, second->relation);
    
    /*Check if relations exist in the mid results and if they are sorted
    and create them accordingly*/
    
    if (lhs_index != -1 && rhs_index == -1) {

        allocate_relation_mid_results(rel, mid_results[lhs_index], metadata_arr[lhs_rel].data[lhs_col]->tuples, 0);

        exists_info exists = relation_exists(mid_results_array, rhs_rel, second->relation);
        if (exists.index == -1) {
            allocate_relation(rel, metadata_arr[rhs_rel].data[rhs_col], 1);
        } else {
            allocate_relation_mid_results(rel, mid_results_array[exists.mid_result][exists.index], metadata_arr[rhs_rel].data[rhs_col]->tuples, 1);
        }

        if (exists.index == -1) {
            if (mid_results[lhs_index].last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_RHS;
            } else {
				mid_results[lhs_index].last_column_sorted = lhs_col;
                return CLASSIC_JOIN;
            }
        }
        else {
            if (mid_results[lhs_index].last_column_sorted == (int32_t) lhs_col && mid_results_array[exists.mid_result][exists.index].last_column_sorted == (int32_t) rhs_col) {
                return SCAN_JOIN;
            }
            else if (mid_results[lhs_index].last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_RHS;
            }
            else if (mid_results_array[exists.mid_result][exists.index].last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_LHS;
            }
            else {
                return CLASSIC_JOIN;
            }
        }
    }
    else if (lhs_index != -1 && rhs_index != -1) {
        allocate_relation_mid_results(rel, mid_results[lhs_index], metadata_arr[lhs_rel].data[lhs_col]->tuples, 0);
        allocate_relation_mid_results(rel, mid_results[rhs_index], metadata_arr[rhs_rel].data[rhs_col]->tuples, 1);

       return SCAN_JOIN;
    }
    else if (lhs_index == -1 && rhs_index != -1) {

        allocate_relation_mid_results(rel, mid_results[rhs_index], metadata_arr[rhs_rel].data[rhs_col]->tuples, 1);

        exists_info exists = relation_exists(mid_results_array, lhs_rel, pred->first.relation);
        if (exists.index == -1) {
            allocate_relation(rel, metadata_arr[lhs_rel].data[lhs_col], 0);
        } else {
            allocate_relation_mid_results(rel, mid_results_array[exists.mid_result][exists.index], metadata_arr[lhs_rel].data[lhs_col]->tuples, 0);
        }
        
        if (exists.index == -1) {
            if (mid_results[rhs_index].last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_LHS;
            } else {
                mid_results[rhs_index].last_column_sorted = rhs_col;
                return CLASSIC_JOIN;
            }
        }
        else {
            if (mid_results[rhs_index].last_column_sorted == (int32_t) rhs_col && mid_results_array[exists.mid_result][exists.index].last_column_sorted == (int32_t) lhs_col) {
                return SCAN_JOIN;
            }
            else if (mid_results[rhs_index].last_column_sorted == (int32_t) rhs_col) {
                return JOIN_SORT_LHS;
            }
            else if (mid_results_array[exists.mid_result][exists.index].last_column_sorted == (int32_t) lhs_col) {
                return JOIN_SORT_RHS;
            }
            else {
                return CLASSIC_JOIN;
            }
        }
        
    }
    else if (lhs_index == -1 && rhs_index == -1) {

        allocate_relation(rel, metadata_arr[rhs_rel].data[rhs_col], 1);
        allocate_relation(rel, metadata_arr[lhs_rel].data[lhs_col], 0);

        mid_result *mid_res = NULL;
        buf_push((*mid_results_array_ptr), mid_res);

        if (rhs_rel != lhs_rel || pred->first.relation != second->relation) {
            return CLASSIC_JOIN;
        }
        else {
            return SCAN_JOIN;
        }
    }

	return 0;
}

static void fix_all_mid_results(mid_result **mid_results_array, ssize_t mid_result_index, ssize_t index, mid_result *current_mid_res, uint64_t *join_payloads, bool is_scan) {

    for (size_t i = 0 ; i < buf_len(mid_results_array[mid_result_index]) ; i++) {
        uint64_t *old_payloads = mid_results_array[mid_result_index][i].payloads;
        mid_results_array[mid_result_index][i].payloads = NULL;
        for (size_t j = 0 ; j < buf_len(join_payloads) ; j++) {
            buf_push(mid_results_array[mid_result_index][i].payloads, old_payloads[join_payloads[j]]);
        }
            
        buf_free(old_payloads);
    }
    if (!is_scan) {
        mid_results_array[mid_result_index][index].last_column_sorted = current_mid_res->last_column_sorted;
        mid_results_array[mid_result_index][index].predicate_id = current_mid_res->predicate_id;
        mid_results_array[mid_result_index][index].relation = current_mid_res->relation;
    }
}

void update_mid_results(mid_result **mid_results_array, metadata *metadata_arr, join_info info) {

    mid_result tmp_R;
    tmp_R.last_column_sorted = info.colR;
    tmp_R.relation = info.relR;
    tmp_R.payloads = info.join_res.results[0];
    tmp_R.predicate_id = info.predR_id;

    mid_result tmp_S;
    tmp_S.last_column_sorted = info.colS;
    tmp_S.relation = info.relS;
    tmp_S.payloads = info.join_res.results[1];
    tmp_S.predicate_id = info.predS_id;

    int join_id = info.join_id;

    if (join_id == CLASSIC_JOIN) {
        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index != -1) {
            fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R ,info.join_res.results[0], 0);
            buf_free(tmp_R.payloads);
        }

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index != -1) {
            fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S ,info.join_res.results[1], 0);
            buf_free(tmp_S.payloads);
        }

        if (exists_r.index == -1) {
            buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_R);  
        }
        if (exists_s.index == -1) {
            buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_S);
        }
    }
    else if (join_id == JOIN_SORT_LHS) {

        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index != -1) {
            fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R, info.join_res.results[0], 0);
            buf_free(tmp_R.payloads);
        }

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        } 

        fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S, info.join_res.results[1], 0);

        buf_free(tmp_S.payloads);

        if (exists_r.index == -1) {
            buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_R);
        }
    }
    else if (join_id == JOIN_SORT_RHS) {

        exists_info exists_s = relation_exists(mid_results_array, info.relS, info.predS_id);
        if (exists_s.index != -1) {
            fix_all_mid_results(mid_results_array, exists_s.mid_result, exists_s.index, &tmp_S, info.join_res.results[1], 0);
            buf_free(tmp_S.payloads);
        }

        exists_info exists_r = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists_r.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        fix_all_mid_results(mid_results_array, exists_r.mid_result, exists_r.index, &tmp_R, info.join_res.results[0], 0);
        buf_free(tmp_R.payloads);

        if (exists_s.index == -1) {
            buf_push(mid_results_array[buf_len(mid_results_array) - 1], tmp_S);
        }
    }
    else if (join_id == SCAN_JOIN) {
        exists_info exists = relation_exists(mid_results_array, info.relR, info.predR_id);

        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }

        fix_all_mid_results(mid_results_array, exists.mid_result, -1, NULL, info.join_res.results[0], 1);
        buf_free(info.join_res.results[0]);
    }
}
