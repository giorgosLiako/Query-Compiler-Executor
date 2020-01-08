#include "queries.h"

typedef struct exec_query_args {
    query *qry;
    DArray *metadata_arr;
} exec_query_args;

static inline void swap_predicates(query *qry, ssize_t i, ssize_t j) {

    if (i == j) {
        return;
    }

    predicate temp = qry->predicates[i];
    qry->predicates[i] = qry->predicates[j];
    qry->predicates[j] = temp;
}

static inline bool is_match(predicate *lhs, predicate *rhs) {
   

    relation_column *lhs_second = (relation_column *) lhs->second;
    relation_column *rhs_second = (relation_column *) rhs->second;
    if (lhs->first.column == rhs->first.column && lhs->first.relation == rhs->first.relation) {
        return true;
    }
    else if (lhs->first.column == rhs_second->column && lhs->first.relation == rhs_second->relation) {
        return true;
    }
    else if (lhs_second->column == rhs->first.column && lhs_second->relation == rhs->first.relation) {
        return true;
    }
    else if (lhs_second->column == rhs_second->column && lhs_second->relation == rhs_second->relation) {
        return true;
    }

    return false;
}

static inline void group_matches(query *qry, ssize_t index) {

    for (ssize_t i = index ; i < (ssize_t) qry->predicates_size - 1 ; ) {
        predicate *current = &(qry->predicates[i]);
        bool swapped = false;
        for (ssize_t j = i + 1 ; j < (ssize_t) qry->predicates_size ; j++) {
            predicate *next = &(qry->predicates[j]);
            if (is_match(current, next)) {
                swap_predicates(qry, ++index, j);
                swapped = true;
            }
        }
        if (swapped) {
            i += index - i;
        } else {
            i++;
        }
    }
}

static ssize_t group_filters(query * qry) {

    ssize_t index = 0 ;
    for (ssize_t i = 1 ; i < (ssize_t) qry->predicates_size  ; i++){
        
        if( qry->predicates[i].type == 1) {
            ssize_t swaps = i; 
            for(ssize_t j=0 ; j < i-index ; j++){
                
                swap_predicates(qry, swaps , swaps-1); 
                swaps--;
            }
            index++;
        } 
    }
    return index;         
}

void arrange_predicates(query *qry) {

    ssize_t index = group_filters(qry);

    group_matches(qry, index);
}

static void* execute_query(void *arg) {
 
    exec_query_args *args = (exec_query_args *) arg;

    query *qry = args->qry;
    arrange_predicates(qry);

    DArray *mid_results_array = DArray_create(sizeof(DArray *), 2);

    for (size_t i = 0 ; i < (size_t) qry->predicates_size ; i++) {
        if (qry->predicates[i].type == 0) {
            execute_join(&qry->predicates[i], qry->relations, args->metadata_arr, mid_results_array);
        }
        else {
            execute_filter(&qry->predicates[i], qry->relations, args->metadata_arr, mid_results_array);
        }
    }

    return NULL;
}

void execute_queries(DArray *q_list, DArray *metadata_arr, thr_pool_t *pool) {

    DArray *args_array = DArray_create(sizeof(exec_query_args), 1000);

    for (size_t i = 0; i < DArray_count(q_list); i++) {

        query *current = (query*) DArray_get(q_list, i);

        exec_query_args args;
        args.metadata_arr = metadata_arr;
        args.qry = current;
        DArray_push(args_array, &args);
 
        thr_pool_queue(pool, execute_query, (void *) DArray_last(args_array));
    }
    thr_pool_barrier(pool);

    DArray_destroy(args_array);
}

