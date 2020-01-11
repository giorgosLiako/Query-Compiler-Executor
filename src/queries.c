#include "queries.h"
#include "filter.h"
#include "join.h"
#include "quicksort.h"

typedef struct exec_query_args {
    query *qry;
    DArray *metadata_arr;
    thr_pool_t *pool;
    DArray **mid_results_arrays;
    uint32_t job_id;
} exec_query_args;

static void free_mid_result_arrays(DArray **mid_results_arrays, ssize_t num_arrays) {

    for (ssize_t curr = 0 ; curr < num_arrays ; curr++) {
        DArray *mid_results_array = mid_results_arrays[curr];
        for (size_t j = 0 ; j < DArray_count(mid_results_array) ; j++) {
            DArray *mid_results = *(DArray **) DArray_get(mid_results_array, j);
            for (size_t i = 0 ; i < DArray_count(mid_results) ; i++) {
                mid_result *res = (mid_result *) DArray_get(mid_results, i);
                DArray_destroy(res->tuples);
            }
            DArray_destroy(mid_results);
        }
        DArray_destroy(mid_results_array);
    }
    FREE(mid_results_arrays);
}

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

static void print_sums(DArray *mid_results_array, uint32_t *relations, DArray *metadata_arr, relation_column *selects, size_t select_size) {

    for (size_t i = 0 ; i < select_size ; i++) {
        uint32_t rel = relations[selects[i].relation];
        uint32_t col = selects[i].column;

        exists_info exists = relation_exists(mid_results_array, rel, selects[i].relation);
        if (exists.index == -1) {
            log_err("Something went really wrong...");
            exit(EXIT_FAILURE);
        }
        DArray *mid_results = *(DArray **) DArray_get(mid_results_array, exists.mid_result);
        mid_result *res = DArray_get(mid_results, exists.index);

        if (DArray_count(res->tuples) == 0) {
            printf("%s", "NULL ");
        } else {
            uint64_t sum = 0;
            DArray *tuples = ((metadata *) DArray_get(metadata_arr, rel))->data[col]->tuples;
            for (ssize_t j = 0 ; j < DArray_count(res->tuples) ; j++) {
               sum += ((tuple *) DArray_get(tuples, ((tuple *) DArray_get(res->tuples, j))->payload))->key;
            }
            printf("%lu ", sum);
        }
    }
    printf("\n");
}

void print_relations(uint32_t* relations, size_t size){
    printf("Relations: \n\t");
    for (size_t i = 0; i < size; i++) {
        printf("%u ", relations[i]);
    }
    printf("\n");
}

void print_predicates(predicate* predicates, size_t size) {
    printf("Predicates: \n");
    for (size_t i = 0; i < size; i++) {
        if (predicates[i].type == 0) {
            relation_column *temp = (relation_column*) predicates[i].second;
            printf("\t%lu.%lu %c %lu.%lu\n", predicates[i].first.relation, predicates[i].first.column, predicates[i].operator
                    , temp->relation, temp->column);
        } else if (predicates[i].type == 1) {
            int *temp = (int*) predicates[i].second;
            printf("\t%lu.%lu %c %d\n", predicates[i].first.relation, predicates[i].first.column, predicates[i].operator
                    , *temp);
        }
    }
}

void print_select(relation_column* r_c, size_t size){
    printf("Select: \n");
    for (size_t i = 0; i < size; i++){
        printf("\t%lu.%lu ", r_c[i].relation, r_c[i].column);
    }
    printf("\n");
    
}


#ifndef MULTITHREADING
    static int execute_query(query* q , DArray* metadata_arr, thr_pool_t *pool) {

        DArray *mid_results_array = DArray_create(sizeof(DArray *), 2);

        print_predicates(q->predicates, q->predicates_size);
        print_select(q->selects, q->select_size);
    
        for (size_t i = 0 ; i < (size_t)q->predicates_size ; i++) {

            if( q->predicates[i].type == 1) { //filter predicate
                check(execute_filter(&q->predicates[i], q->relations, metadata_arr, mid_results_array) != -1, "");
            } else {    //join predicate
                check(execute_join(&q->predicates[i], q->relations, metadata_arr, mid_results_array, pool) != -1, "Join failed!");
            }
        }

        print_sums(mid_results_array, q->relations, metadata_arr, q->selects, q->select_size);

        for (size_t j = 0 ; j < DArray_count(mid_results_array) ; j++) {
            DArray *mid_results = *(DArray **) DArray_get(mid_results_array, j);
            for (size_t i = 0 ; i < DArray_count(mid_results) ; i++) {
                mid_result *res = (mid_result *) DArray_get(mid_results, i);
                DArray_destroy(res->tuples);
            }    
            DArray_destroy(mid_results);
        }
        DArray_destroy(mid_results_array);

        return 0;

        error:
            return -1;
    }


    void execute_queries(DArray *q_list, DArray *metadata_arr, thr_pool_t *pool) {


        for (size_t i = 0; i < DArray_count(q_list); i++) {

            query *tmp_data = (query*) DArray_get(q_list, i);

            arrange_predicates(tmp_data);

            execute_query(tmp_data , metadata_arr, pool);
        }
    }
#else

    static void* execute_query(void *arg) {
    
        exec_query_args *args = (exec_query_args *) arg;

        query *qry = args->qry;
        arrange_predicates(qry);

        DArray *mid_results_array = DArray_create(sizeof(DArray *), 2);

        for (size_t i = 0 ; i < (size_t) qry->predicates_size ; i++) {
            if (qry->predicates[i].type == 0) {
                execute_join(&qry->predicates[i], qry->relations, args->metadata_arr, mid_results_array, args->pool);
            }
            else {
                execute_filter(&qry->predicates[i], qry->relations, args->metadata_arr, mid_results_array);
            }
        }
        args->mid_results_arrays[args->job_id] = mid_results_array;

        return NULL;
    }

    void execute_queries(DArray *q_list, DArray *metadata_arr, thr_pool_t *pool) {

        DArray *args_array = DArray_create(sizeof(exec_query_args), DArray_count(q_list));

        DArray **mid_results_arrays = MALLOC(DArray *, DArray_count(q_list));

        thr_pool_t *inner_pool = thr_pool_create(4);
    
        for (size_t i = 0; i < DArray_count(q_list); i++) {

            query *current = (query*) DArray_get(q_list, i);

            exec_query_args args;
            args.metadata_arr = metadata_arr;
            args.qry = current;
            args.pool = inner_pool;
            args.mid_results_arrays = mid_results_arrays;
            args.job_id = i;
            DArray_push(args_array, &args);
    
            thr_pool_queue(pool, execute_query, (void *) DArray_last(args_array));
        }        
        thr_pool_barrier(pool);

        thr_pool_destroy(inner_pool);
        
        for (size_t i = 0 ; i < DArray_count(q_list) ; i++) {
            exec_query_args *args = (exec_query_args *) DArray_get(args_array, i);
            query *current = (query *) DArray_get(q_list, i);
            
            print_sums(args->mid_results_arrays[i], current->relations, metadata_arr, current->selects, current->select_size);
        }

        free_mid_result_arrays(mid_results_arrays, DArray_count(q_list));

        DArray_destroy(args_array);

    }

#endif
