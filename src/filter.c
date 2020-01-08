#include "filter.h"

static void apply_filter(int filter_type, bool rel_exists, mid_result *mid_res, DArray *rel_tuples, uint32_t number) {

    ssize_t iterations = rel_exists ? DArray_count(mid_res->tuples) : rel->num_tuples;
    for (ssize_t i = 0 ; i < iterations ; i++) {

        if (filter_type == EQ) {
            if (rel_exists) {
                tuple *tup = (tuple *) DArray_get(mid_res->tuples, i);
                if (tup->key != number) {
                    DArray_remove(mid_res->tuples, i);
                    i--;
                }
            }
            else {
                if (((tuple *) DArray_get(rel_tuples, i))->key == number) {
                    DArray_push(mid_res->tuples, &(*((tuple *) DArray_get(rel_tuples, i))));
                }
            }
        }
        else if (filter_type == LEQ) {
            if (rel_exists) {
                tuple *tup = (tuple *) DArray_get(mid_res->tuples, i);
                if (!(tup->key <= number)) {
                    DArray_remove(mid_res->tuples, i);
                    i--;
                }
            }
            else {
                if (((tuple *) DArray_get(rel_tuples, i))->key <= number) {
                    DArray_push(mid_res->tuples, &(*((tuple *) DArray_get(rel_tuples, i))));
                }
            }
        }
        else if (filter_type == GEQ) {
            if (rel_exists) {
                tuple *tup = (tuple *) DArray_get(mid_res->tuples, i);
                if (!(tup->key >= number)) {
                    DArray_remove(mid_res->tuples, i);
                    i--;
                }
            }
            else {
                if (((tuple *) DArray_get(rel_tuples, i))->key >= number) {
                    DArray_push(mid_res->tuples, &(*((tuple *) DArray_get(rel_tuples, i))));
                }
            }
        }
        else if (filter_type == L) {
            if (rel_exists) {
                tuple *tup = (tuple *) DArray_get(mid_res->tuples, i);
                if (!(tup->key < number)) {
                    DArray_remove(mid_res->tuples, i);
                    i--;
                } 
            }
            else {
                if (((tuple *) DArray_get(rel_tuples, i))->key < number) {
                    DArray_push(mid_res->tuples, &(*((tuple *) DArray_get(rel_tuples, i))));
                }
            }
        }
        else if (filter_type == G) {
            if (rel_exists) {
                tuple *tup = (tuple *) DArray_get(mid_res->tuples, i);
                if (!(tup->key > number)) {
                    DArray_remove(mid_res->tuples, i);
                    i--;
                }
            }
            else {
                if (((tuple *) DArray_get(rel_tuples, i))->key > number) {
                    DArray_push(mid_res->tuples, &(*((tuple *) DArray_get(rel_tuples, i))));
                }
            }
        }
    }
}

int execute_filter(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results_array) {

    metadata *tmp_data = (metadata*) DArray_get(metadata_arr, relations[pred->first.relation]);
    relation *rel = tmp_data->data[pred->first.column];
    uint64_t *number = (uint64_t*) pred->second; 

    DArray *mid_results = NULL;
    if (DArray_count(mid_results_array) == 0) {
        mid_results = DArray_create(sizeof(mid_result), 4);
        check_mem(mid_results);
        DArray_push(mid_results_array, &mid_results);
    } 
    else { 
        mid_results = * (DArray **) DArray_last(mid_results_array);
    }

    exists_info exists = relation_exists(mid_results_array, relations[pred->first.relation], pred->first.relation);
    mid_result *current;
    if (exists.index != -1)  {
        DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        current = (mid_result *) DArray_get(mid_results, exists.index);
        
        apply_filter(pred->operator, 1, current, rel, *number);
    } else {
        mid_result res;
        res.relation = relations[pred->first.relation];
        res.predicate_id = pred->first.relation;
        res.last_column_sorted = -1;
        res.last_column_accessed = pred->first.column;
        res.tuples = DArray_create(sizeof(tuple), 2000);
        DArray_push(mid_results, &res);

        current = (mid_result *) DArray_last(mid_results);

        apply_filter(pred->operator, 0, current, rel->tuples, *number);
    }

    return 0;

    error:
        return -1;
}