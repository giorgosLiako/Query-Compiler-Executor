#include "pred_arrange.h"

static inline void swap_predicates(query *qry, ssize_t i, ssize_t j) {

    if (i == j) {
        return;
    }

    predicate temp = qry->predicates[i];
    qry->predicates[i] = qry->predicates[j];
    qry->predicates[j] = temp;
}

static inline bool is_identical(predicate *lhs, predicate *rhs) {

    relation_column *lhs_second = (relation_column *) lhs->second;
    relation_column *rhs_second = (relation_column *) rhs->second;

    if (lhs->first.relation == rhs->first.relation && lhs_second->relation == rhs_second->relation) {
        return true;
    }
    if (lhs->first.relation == rhs_second->relation && lhs_second->relation == rhs->first.relation) {
        return true;
    }

    return false;
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