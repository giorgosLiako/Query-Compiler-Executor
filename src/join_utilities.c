#include "join_utilities.h"
#include "quicksort.h"

static void create_entity_mid_results(DArray *mid_results_array) {

    DArray *mid_results = DArray_create(sizeof(mid_result), 4);

    DArray_push(mid_results_array, &mid_results);
}

bool is_sorted(DArray *rel) {
	for (ssize_t i = 0 ; i < DArray_count(rel) - 1 ; i++) {
		tuple *tup = (tuple *) DArray_get(rel, i);
		tuple *tup_1 = (tuple *) DArray_get(rel, i + 1);
		if (tup->key > tup_1->key) {
			return false;
		}
	}
	return true;
}

static void create_new_queue(DArray *tuples, DArray **retval, uint32_t *jobs_created) {
	
	DArray *reordered = allocate_reordered_darray(tuples);
    DArray *q = DArray_create(sizeof(queue_node), 256);
	
    DArray *relations[2]; //to swap after each iteration

    histogram new_h, new_p;
    //build the first histogram and the first psum ( of all the array)
    build_histogram_darray(tuples, &new_h, 1, 0, DArray_count(tuples));
    build_psum(&new_h, &new_p);
    //build the R' (reordered R)
    reordered = build_reordered_darray(reordered, tuples, &new_h, &new_p, 1, 0, DArray_count(tuples));
    
    relations[0] = tuples;
    relations[1] = reordered;

    //the byte take the values [0-255] , each value is a bucket
    //each bucket that found push it in the queue
    DArray *q_to_return = DArray_create(sizeof(queue_node), 256);
    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0) {
            queue_node q_node;
            q_node.byte = j;
            q_node.base = new_p.array[j];
            q_node.size = new_h.array[j];
            DArray_push(q, &q_node);
        }
    }

    ssize_t i;
    int number_of_buckets = 0;
    //above we execute the routine for the first byte
    //now for all the other bytes
    for (i = 2; i <= 8 ; i++) {
        
        number_of_buckets = DArray_count(q);
        if (number_of_buckets >= 2) {
            for (ssize_t tmp_i = 0 ; tmp_i < DArray_count(q) ; tmp_i++) {
                queue_node q_node = *(queue_node *) DArray_get(q, tmp_i);
                DArray_push(q_to_return, &q_node);
            }
			break;
        }

        //the size of the queue is the number of the buckets
        while (number_of_buckets) { //for each bucket
    
            queue_node *current_item = DArray_get(q, 0);
            ssize_t base = current_item->base, size = current_item->size; // base = start of bucket , size = the number of cells of the bucket
            DArray_remove(q, 0);
          
            number_of_buckets--;
            //check if the bucket is smaller than 32 KB to execute quicksort
            if ( size*sizeof(tuple) + sizeof(uint64_t) >= 32*1024) {
				// if the bucket is bigger than 32 KB , sort by the next byte
                histogram new_h, new_p;
                //build again histogram and psum of the next byte
                build_histogram_darray(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                //build the reordered array of the previous bucket
                relations[i%2] = build_reordered_darray(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                //push the buckets to the queue
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0) {
                        queue_node q_node;
                        q_node.byte = j;
                        q_node.base = base + new_p.array[j];
                        q_node.size = new_h.array[j];
                        DArray_push(q, &q_node);
                    }
                }
			}
        }
    }
	number_of_buckets = DArray_count(q);
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

    DArray_destroy(reordered);
    DArray_destroy(q);
}

static inline void allocate_relation_mid_results(mid_result *mid_res, DArray *metadata_arr, uint32_t rel, uint32_t col, rel_info *info[2], ssize_t index) {
	
	DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, rel))->data[col]->tuples;
	
	info[index]->rel = DArray_create(sizeof(tuple), DArray_count(mid_res->tuples) + 1);
	for (ssize_t i = 0 ; i < DArray_count(mid_res->tuples) ; i++) {
		uint64_t payload = ((tuple *) DArray_get(mid_res->tuples, i))->payload;
		tuple to_push; 
		to_push.key = ((tuple *) DArray_get(tmp_tuples, payload))->key;
		to_push.payload = payload;
		DArray_push(info[index]->rel, &to_push);
	}
	info[index]->destroy_rel = true;
}

int sort_relations(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array, rel_info *rel[2], thr_pool_t *pool) {

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

	if (lhs_index != -1 && rhs_index == -1) {
		
		mid_result *mid_res_R = (mid_result *) DArray_get(mid_results, lhs_index);
		
		if (mid_res_R->last_column_sorted == lhs_col) {
			rel[0]->rel = mid_res_R->tuples;
			rel[0]->destroy_rel = false;
		}
		else {
			/*DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, lhs_rel))->data[lhs_col]->tuples;
			
			rel[0]->rel = DArray_create(sizeof(tuple), DArray_count(mid_res_R->tuples) + 1);
			for (ssize_t i = 0 ; i < DArray_count(mid_res_R->tuples) ; i++) {
				uint64_t payload = ((tuple *) DArray_get(mid_res_R->tuples, i))->payload;
				tuple to_push; 
				to_push.key = ((tuple *) DArray_get(tmp_tuples, payload))->key;
				to_push.payload = payload;
				DArray_push(rel[0]->rel, &to_push);
			}
			rel[0]->destroy_rel = true;*/
			allocate_relation_mid_results(mid_res_R, metadata_arr, lhs_rel, lhs_col, rel, 0);
		}
		
		exists_info exists = relation_exists(mid_results_array, rhs_rel, second->relation);
		DArray *tmp_mid_results = NULL;
		
		if (exists.index != -1) {

			tmp_mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
			mid_result *mid_res_S = (mid_result *) DArray_get(tmp_mid_results, exists.index);
			if (mid_res_S->last_column_sorted == rhs_col) {
				rel[1]->rel = mid_res_S->tuples;
				rel[1]->destroy_rel = false;
			}
			else {
				allocate_relation_mid_results(mid_res_S, metadata_arr, rhs_rel, rhs_col, rel, 1);
			}
		}
		else {
			DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, rhs_rel))->data[rhs_col]->tuples;
			rel[1]->rel = DArray_create(sizeof(tuple), DArray_count(tmp_tuples));
			for (ssize_t i = 0 ; i < DArray_count(tmp_tuples) ; i++) {
				DArray_push(rel[1]->rel, DArray_get(tmp_tuples, i));
			}
			rel[1]->destroy_rel = true;
		}

		mid_result *mid_res = (mid_result *) DArray_get(mid_results, lhs_index);
		if (tmp_mid_results == NULL) {
			if (mid_res->last_column_sorted == (int32_t) lhs_col) {
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				
				create_new_queue(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create));
				
				return JOIN_SORT_RHS;
			}
			else {
				mid_res->last_column_sorted = lhs_col;
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				
				return CLASSIC_JOIN;
			}
		}
		else {
			mid_result *tmp_mid_result = (mid_result *) DArray_get(tmp_mid_results, exists.index);
			if (mid_res->last_column_sorted == (int32_t) lhs_col && tmp_mid_result->last_column_sorted == (int32_t) rhs_col) {
				return SCAN_JOIN;
			}
			else if (mid_res->last_column_sorted == (int32_t) lhs_col) {
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);

				create_new_queue(rel[0]->rel, &rel[0]->queue, &rel[0]->jobs_to_create);		
				
				return JOIN_SORT_RHS;
			}
			else if (tmp_mid_result->last_column_sorted == (int32_t) rhs_col) {
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);

				create_new_queue(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create));
				
				return JOIN_SORT_LHS;
			}
			else {
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				
				return CLASSIC_JOIN;
			}
		}
	}
	else if (lhs_index != -1 && rhs_index != -1) {

		mid_result *mid_res_R = (mid_result *) DArray_get(mid_results, lhs_index);
		mid_result *mid_res_S = (mid_result *) DArray_get(mid_results, rhs_index);
		if (mid_res_R->last_column_sorted == lhs_col) {
			rel[0]->rel = mid_res_R->tuples;
			rel[0]->destroy_rel = false;
		}
		else {
			/*DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, lhs_rel))->data[lhs_col]->tuples;

			rel[0]->rel = DArray_create(sizeof(tuple), DArray_count(mid_res_R->tuples) + 1);
			for (ssize_t i = 0 ; i < DArray_count(mid_res_R->tuples) ; i++) {
				uint64_t payload = ((tuple *) DArray_get(mid_res_R->tuples, i))->payload;
				tuple to_push;
				to_push.key = ((tuple *) DArray_get(tmp_tuples, payload))->key;
				to_push.payload = payload;
				DArray_push(rel[0]->rel, &to_push);
			}
			rel[0]->destroy_rel = true;*/
			allocate_relation_mid_results(mid_res_R, metadata_arr, lhs_rel, lhs_col, rel, 0);
		}
		
		if (mid_res_S->last_column_sorted == rhs_col) {
			rel[1]->rel = mid_res_S->tuples;
		}
		else {
			allocate_relation_mid_results(mid_res_S, metadata_arr, rhs_rel, rhs_col, rel, 1);
		}

		rel[0]->destroy_rel = false;
		rel[1]->destroy_rel = false;
		rel[0]->queue = NULL;
		rel[1]->queue = NULL;

		return SCAN_JOIN;
	}
	else if (lhs_index == -1 && rhs_index != -1) {

		mid_result *mid_res_S = (mid_result *) DArray_get(mid_results, rhs_index);
		if (mid_res_S->last_column_sorted == rhs_col) {
			rel[1]->rel = mid_res_S->tuples;
			rel[1]->destroy_rel = false;
		}
		else {
			/*DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, rhs_rel))->data[rhs_col]->tuples;
			
			rel[1]->rel = DArray_create(sizeof(tuple), DArray_count(mid_res_S->tuples) + 1);
			for (ssize_t i = 0 ; i < DArray_count(mid_res_S->tuples) ; i++) {
				uint64_t payload = ((tuple *) DArray_get(mid_res_S->tuples, i))->payload;
				tuple to_push; 
				to_push.key = ((tuple *) DArray_get(tmp_tuples, payload))->key;
				to_push.payload = payload;
				DArray_push(rel[1]->rel, &to_push);
			}
			rel[1]->destroy_rel = true;*/
			allocate_relation_mid_results(mid_res_S, metadata_arr, rhs_rel, rhs_col, rel, 1);
		}

		exists_info exists = relation_exists(mid_results_array, lhs_rel, pred->first.relation);
		DArray *tmp_mid_results = NULL;
		if (exists.index != -1) {
			
			tmp_mid_results = * (DArray **) DArray_get(mid_results_array, exists.mid_result);
			mid_result *mid_res_R = (mid_result *) DArray_get(tmp_mid_results, exists.index);
			
			if (mid_res_R->last_column_sorted == lhs_col) {
				rel[0]->rel = mid_res_R->tuples;
				rel[0]->destroy_rel = false;
			}
			else {
				allocate_relation_mid_results(mid_res_R, metadata_arr, lhs_rel, lhs_col, rel, 0);
			}
		}
		else {
			DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, lhs_rel))->data[lhs_col]->tuples;
			rel[0]->rel = DArray_create(sizeof(tuple), DArray_count(tmp_tuples));
			for (ssize_t i = 0 ; i < DArray_count(tmp_tuples) ; i++) {
				DArray_push(rel[0]->rel, DArray_get(tmp_tuples, i));
			}
			rel[0]->destroy_rel = true;
		}

		mid_result *mid_res = (mid_result *) DArray_get(mid_results, rhs_index);
		if (tmp_mid_results == NULL) {
			if (mid_res->last_column_sorted == (int32_t) rhs_col) {
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
				
				create_new_queue(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create));

				return JOIN_SORT_LHS;
			}
			else {
				mid_res->last_column_sorted = rhs_col;
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				return CLASSIC_JOIN;
			}
		}
		else {
			mid_result *tmp_mid_result = (mid_result *) DArray_get(tmp_mid_results, exists.index);
			if (mid_res->last_column_sorted == (int32_t) rhs_col && tmp_mid_result->last_column_sorted == (int32_t) lhs_col) {
				return SCAN_JOIN;
			}
			else if (mid_res->last_column_sorted == (int32_t) rhs_col) {
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);

				create_new_queue(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create));

				return JOIN_SORT_LHS;
			}
			else if (tmp_mid_result->last_column_sorted == (int32_t) lhs_col) {
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				
				create_new_queue(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create));

				return JOIN_SORT_RHS;
			}
			else {
				iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
				iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
				return CLASSIC_JOIN;
			}
		}
	}
	else if (lhs_index == -1 && rhs_index == -1) {
		create_entity_mid_results(mid_results_array);

		DArray *tmp_tuples = ((metadata *) DArray_get(metadata_arr, lhs_rel))->data[lhs_col]->tuples;
		rel[0]->rel = DArray_create(sizeof(tuple), DArray_count(tmp_tuples));
		for (ssize_t i = 0 ; i < DArray_count(tmp_tuples) ; i++) {
			DArray_push(rel[0]->rel, DArray_get(tmp_tuples, i));
		}
		rel[0]->destroy_rel = true;

		tmp_tuples = ((metadata *) DArray_get(metadata_arr, rhs_rel))->data[rhs_col]->tuples;
		rel[1]->rel = DArray_create(sizeof(tuple), DArray_count(tmp_tuples));
		for (ssize_t i = 0 ; i < DArray_count(tmp_tuples) ; i++) {
			DArray_push(rel[1]->rel, DArray_get(tmp_tuples, i));
		}
		rel[1]->destroy_rel = true;

		if (rhs_rel != lhs_rel || pred->first.relation != second->relation) {
			iterative_sort(rel[0]->rel, &(rel[0]->queue), &(rel[0]->jobs_to_create), pool);
			iterative_sort(rel[1]->rel, &(rel[1]->queue), &(rel[1]->jobs_to_create), pool);
			return CLASSIC_JOIN;
		}
		else {
			return SCAN_JOIN;
		}
	}
	
	return -1;
}

static void single_threaded_sort(DArray *tuples) {

	if (DArray_count(tuples) == 0) {
		return;
	}

	if ( DArray_count(tuples) * sizeof(tuple) + sizeof(uint64_t) < 64*1024) {
        random_quicksort(tuples, 0, DArray_count(tuples) - 1);
    }
    
	DArray *reordered = allocate_reordered_darray(tuples);
    DArray *q = DArray_create(sizeof(queue_node), 256);
    check_mem(q);

    DArray *relations[2]; //to swap after each iteration

    histogram new_h, new_p;
    //build the first histogram and the first psum ( of all the array)
    build_histogram_darray(tuples, &new_h, 1, 0, DArray_count(tuples));
    build_psum(&new_h, &new_p);
    //build the R' (reordered R)
    reordered = build_reordered_darray(reordered, tuples, &new_h, &new_p, 1, 0, DArray_count(tuples));
    
    relations[0] = tuples;
    relations[1] = reordered;

    //the byte take the values [0-255] , each value is a bucket
    //each bucket that found push it in the queue
    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0) {
            queue_node q_node;
            q_node.byte = j;
            q_node.base = new_p.array[j];
            q_node.size = new_h.array[j];
            DArray_push(q, &q_node);
        }
    }

    ssize_t i;
    int number_of_buckets = 0;
    //above we execute the routine for the first byte
    //now for all the other bytes
	int count = 1;
    for (i = 2; i <= 8 ; i++) {
        
        number_of_buckets = DArray_count(q);
        if (!(number_of_buckets > 0)) {
			break;
		}
		else {
			count++;
		}

        //the size of the queue is the number of the buckets
        while (number_of_buckets) { //for each bucket
    
            queue_node *current_item = (queue_node *) DArray_get(q, 0);
            ssize_t base = current_item->base, size = current_item->size; // base = start of bucket , size = the number of cells of the bucket
            DArray_remove(q, 0);
          
            number_of_buckets--;
            //check if the bucket is smaller than 32 KB to execute quicksort
            if ( size*sizeof(tuple) + sizeof(uint64_t) < 32*1024) {
                random_quicksort(relations[(i + 1) % 2], base, base + size - 1);
                
                for (ssize_t j = base; j < base + size; j++) {

                    tuple *tup_1 = (tuple *) DArray_get(relations[i%2], j);
                    tuple *tup_2 = (tuple *) DArray_get(relations[(i + 1)%2], j);

                    tup_1->key = tup_2->key;
                    tup_1->payload = tup_2->payload;
                }

            } else { // if the bucket is bigger than 32 KB , sort by the next byte
                histogram new_h, new_p;
                //build again histogram and psum of the next byte
                build_histogram_darray(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                //build the reordered array of the previous bucket
                relations[i%2] = build_reordered_darray(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                //push the buckets to the queue
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0) {
                        queue_node q_node;
                        q_node.byte = j;
                        q_node.base = base + new_p.array[j];
                        q_node.size = new_h.array[j];
                        DArray_push(q, &q_node);
                    }
                }
            }
        }
    }
	
	DArray_destroy(reordered);
	DArray_destroy(q);
	error:
		return;
}

static DArray* join_payloads(DArray *driver, DArray *last, DArray *edit, DArray *tuples) {
	
	DArray *relR_tuples = DArray_create(sizeof(tuple), DArray_count(last) + 1);
	for (size_t i = 0 ; i < DArray_count(last) ; i++) {
		tuple *tup_last = (tuple *) DArray_get(last, i);
		tuple *tup_edit = (tuple *) DArray_get(edit, i);
		tuple to_push;
		
		to_push.key = tup_last->payload;
		to_push.payload = tup_edit->payload;
		DArray_push(relR_tuples, &to_push);
	}

	DArray *relS_tuples = DArray_create(sizeof(tuple), DArray_count(driver) + 1);
	for (size_t i = 0 ; i < DArray_count(driver) ; i++) {
		tuple to_push;		
		to_push.key = *(uint64_t *) DArray_get(driver, i);
		DArray_push(relS_tuples, &to_push);
	}

	single_threaded_sort(relR_tuples);
	single_threaded_sort(relS_tuples);

	size_t pr = 0;
	size_t s_start = 0;
	DArray *result = DArray_create(sizeof(tuple), 2000);

	while (pr < DArray_count(relR_tuples) && s_start < DArray_count(relS_tuples)) {
		size_t ps = s_start;
		int flag = 0;

		while (ps < DArray_count(relS_tuples)) {
			tuple *tuple_pr = (tuple *) DArray_get(relR_tuples, pr);
			tuple *tuple_ps = (tuple *) DArray_get(relS_tuples, ps);

			if (tuple_pr->key < tuple_ps->key) {
				break;
			}

			if (tuple_pr->key > tuple_ps->key) {
				ps++;
				if (flag == 0) {
					s_start = ps;
				}
			}
			else {
				tuple tup;
				tup.payload = tuple_pr->payload; 
				tup.key = ((tuple *) DArray_get(tuples, tup.payload))->key;
				DArray_push(result, &tup);
				flag = 1;
				ps++;
			}
		}
		pr++;
	}

	DArray_destroy(relS_tuples);
	DArray_destroy(relR_tuples);

	return result;
}

static void fix_all_mid_results(join_info *info, exists_info exists, DArray *mid_results_array, DArray *metadata_arr, mid_result *tmp, int mode) {

	DArray *no_dup = info->join_res.no_duplicates[mode];

	DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
	mid_result *update = (mid_result *) DArray_get(mid_results, exists.index);

	for (size_t i = 0 ; i < DArray_count(mid_results) ; i++) {
		mid_result *edit = (mid_result *) DArray_get(mid_results, i);
		if (edit->relation != info->relR && edit->relation != info->relS) {
			DArray *to_remove = edit->tuples;
			DArray *tuples = ((metadata *) DArray_get(metadata_arr, edit->relation))->data[edit->last_column_sorted]->tuples;
			edit->tuples = join_payloads(no_dup, update->tuples, edit->tuples, tuples);
			DArray_destroy(to_remove);
		}
	}
	mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
	DArray_destroy(destroy->tuples);
	DArray_set(mid_results, exists.index, tmp);
}

void update_mid_results(DArray *mid_results_array, DArray *metadata_arr, join_info info, rel_info *rel[2]) {

	DArray *tuples_R = info.join_res.results[0];
	DArray *tuples_S = info.join_res.results[1];

	int join_id = info.join_id;

	mid_result tmp_R;
	tmp_R.last_column_sorted = info.colR;
	tmp_R.tuples = tuples_R;
	tmp_R.relation = info.relR;
	tmp_R.predicate_id = info.predR_id;

	mid_result tmp_S;
	tmp_S.last_column_sorted = info.colS;
	tmp_S.tuples = tuples_S;
	tmp_S.relation = info.relS;
	tmp_S.predicate_id = info.predS_id;


	if (join_id == CLASSIC_JOIN) {
		/*If we needed to sort both relations, add them to mid results */
		exists_info exists = relation_exists(mid_results_array, info.relR, info.predR_id);
		if (exists.index == -1) {
			DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
			DArray_push(mid_results, &tmp_R);
		}
		else {
			fix_all_mid_results(&info, exists, mid_results_array, metadata_arr, &tmp_R, 0);
		}

		exists = relation_exists(mid_results_array, info.relS, info.predS_id);
		if (exists.index == -1) {
			DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
			DArray_push(mid_results, &tmp_S);
		}
		else {
			fix_all_mid_results(&info, exists, mid_results_array, metadata_arr, &tmp_S, 1);
		}
	}
	else if (join_id == JOIN_SORT_LHS) {
		/*If we sorted only left relation, it means the right one was already in the mid results */

		exists_info exists = relation_exists(mid_results_array, info.relR, info.predR_id);
		if (exists.index == -1) {
			DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
			DArray_push(mid_results, &tmp_R);
		}
		else {
			DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
			mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
			DArray_destroy(destroy->tuples);
			DArray_set(mid_results, exists.index, &tmp_R);
		}

		exists = relation_exists(mid_results_array, info.relS, info.predS_id);
		if (exists.index == -1) {
			log_err("Something went really wrong");
			exit(EXIT_FAILURE);
		}

		fix_all_mid_results(&info, exists, mid_results_array, metadata_arr, &tmp_S, 1);
	}
	else if (join_id == JOIN_SORT_RHS) {

		exists_info exists = relation_exists(mid_results_array, info.relS, info.predS_id);
		if (exists.index == -1) {
			DArray *mid_results = *(DArray **) DArray_last(mid_results_array);
			DArray_push(mid_results, &tmp_S);
		}
		else {
			DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
			mid_result *destroy = (mid_result *) DArray_get(mid_results, exists.index);
			DArray_destroy(destroy->tuples);
			DArray_set(mid_results, exists.index, &tmp_S);
		}

		exists = relation_exists(mid_results_array, info.relR, info.predR_id);
		if (exists.index == -1) {
			log_err("Something went really wrong");
			exit(EXIT_FAILURE);
		}

		fix_all_mid_results(&info, exists, mid_results_array, metadata_arr, &tmp_R, 0);
	}
	else if (join_id == SCAN_JOIN) {
        exists_info exists = relation_exists(mid_results_array, info.relR, info.predR_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
       
        DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        mid_result *update = (mid_result *) DArray_get(mid_results, exists.index);
        update->tuples = tuples_R;

        exists = relation_exists(mid_results_array, info.relS , info.predS_id);
        if (exists.index == -1) {
            log_err("Something went really wrong");
            exit(EXIT_FAILURE);
        }
        
        mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        update = (mid_result *) DArray_get(mid_results, exists.index);
        update->tuples = tuples_S;
	}

	DArray_destroy(info.join_res.no_duplicates[0]);
	DArray_destroy(info.join_res.no_duplicates[1]);
}