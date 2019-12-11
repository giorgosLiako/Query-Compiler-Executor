#include "filter.h"

static int exec_filter_rel_exists(predicate *pred , relation* rel , uint64_t number , mid_result* tmp_results) {  
    //take the saved payloads and check if they satisfy the new filter
    for(size_t i = 0 ; i <  DArray_count(tmp_results->payloads) ; i++) {   
    
        uint64_t* payload = (uint64_t*) DArray_get(tmp_results->payloads,i);
        //every payload that does not satisfy the new filter remove it from the dynamic array 
        if (pred->operator == '=') {
            if (rel->tuples[*payload].key != number) {
                DArray_remove(tmp_results->payloads,i);
                i--; //we removed an item so the i should not increase in this loop
            }
        }
        else if (pred->operator == '>') {
            if (rel->tuples[*payload].key <= number ) { 
                DArray_remove(tmp_results->payloads,i);
                i--;
            } 
        }
        else if (pred->operator == '<') { 
            if (rel->tuples[*payload].key >= number ) {   
                DArray_remove(tmp_results->payloads,i);
                i--;
            }               
        }
        else {   
            log_err("Wrong operator");
            return -1;
        }
    }
    printf("%d\n",DArray_count(tmp_results->payloads));

    return 0;
}

static int exec_filter_rel_no_exists(predicate *pred,relation* rel , uint64_t number ,mid_result* tmp_results) {

    for (size_t i = 0 ; i < rel->num_tuples; i++) {
        
        //if the tuple satisfies the filter push it in the dynamic array of the payloads
        if ( pred->operator == '='){
            if (rel->tuples[i].key == number) {
                DArray_push(tmp_results->payloads, &(rel->tuples[i].payload));
            }
        }
        else if (pred->operator == '>') {
            if (rel->tuples[i].key > number) {
                DArray_push(tmp_results->payloads,  &(rel->tuples[i].payload));
            }
        }
        else if (pred->operator == '<') {
            if (rel->tuples[i].key < number ) {   
                DArray_push(tmp_results->payloads, &( rel->tuples[i].payload));
            }
        }
        else{   
            log_err("Wrong operator");
            return -1;
        }
    }
    printf("%d\n",DArray_count(tmp_results->payloads));

    return 0;
}

int execute_filter(predicate *pred, uint32_t lhs_rel, DArray *metadata_arr, DArray *mid_results) {

    metadata *tmp_data = (metadata*) DArray_get(metadata_arr, lhs_rel);
    relation *rel = tmp_data->data[pred->first.column];
    uint64_t *number = (uint64_t*) pred->second; 

    ssize_t index;
    if ((index = relation_exists(mid_results, lhs_rel)) != -1)  {   
        check(exec_filter_rel_exists(pred, rel, *number, &(*(mid_result *) DArray_get(mid_results, index))) != -1, "Execution of filter failed!");
    } else {
        check(exec_filter_rel_no_exists(pred, rel, *number, &(*(mid_result *) DArray_last(mid_results))) != -1, "Execution of filter failed!");
    }

    return 0;

    error:
        return -1;
}