#include "filter.h"

static int exec_filter_rel_exists(predicate *pred , relation* rel , uint64_t number , DArray *payloads) {  
    //take the saved payloads and check if they satisfy the new filter
    for(size_t i = 0 ; i <  DArray_count(payloads) ; i++) {   
    
        uint64_t* payload = (uint64_t*) DArray_get(payloads,i);
        //every payload that does not satisfy the new filter remove it from the dynamic array 
        if (pred->operator == '=') {
            if (rel->tuples[*payload].key != number) {
                DArray_remove(payloads,i);
                i--; //we removed an item so the i should not increase in this loop
            }
        }
        else if (pred->operator == '>') {
            if (rel->tuples[*payload].key <= number ) { 
                DArray_remove(payloads,i);
                i--;
            } 
        }
        else if (pred->operator == '<') { 
            if (rel->tuples[*payload].key >= number ) {   
                DArray_remove(payloads,i);
                i--;
            }               
        }
        else {   
            log_err("Wrong operator");
            return -1;
        }
    }
    printf("%d\n",DArray_count(payloads));

    return 0;
}

static int exec_filter_rel_no_exists(predicate *pred,relation* rel , uint64_t number ,DArray *payloads) {

    for (size_t i = 0 ; i < rel->num_tuples; i++) {
        
        //if the tuple satisfies the filter push it in the dynamic array of the payloads
        if ( pred->operator == '='){
            if (rel->tuples[i].key == number) {
                DArray_push(payloads, &(rel->tuples[i].payload));
            }
        }
        else if (pred->operator == '>') {
            if (rel->tuples[i].key > number) {
                DArray_push(payloads,  &(rel->tuples[i].payload));
            }
        }
        else if (pred->operator == '<') {
            if (rel->tuples[i].key < number ) {   
                DArray_push(payloads, &( rel->tuples[i].payload));
            }
        }
        else{   
            log_err("Wrong operator");
            return -1;
        }
    }

    return 0;
}

int execute_filter(predicate *pred, uint32_t *relations, DArray *metadata_arr, DArray *mid_results) {

    metadata *tmp_data = (metadata*) DArray_get(metadata_arr, relations[pred->first.relation]);
    relation *rel = tmp_data->data[pred->first.column];
    uint64_t *number = (uint64_t*) pred->second; 

    relation_info *rel_info;
    if ( (rel_info = relation_exists(mid_results, relations[pred->first.relation])) != NULL) {
        exec_filter_rel_exists(pred, rel, *number, rel_info->payloads);
    } else {
        mid_result *res = create_entity(mid_results);
        rel_info = add_to_entity(res, relations[pred->first.relation]);
        rel_info->payloads = DArray_create(sizeof(uint64_t), 50);
        exec_filter_rel_no_exists(pred, rel, *number, rel_info->payloads);
    }   
}
