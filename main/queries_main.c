#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include "../src/alloc_free.h"
#include "../src/structs.h"
#include "../src/dbg.h"
#include "../src/utilities.h"
#include "../src/DArray.h"
#include "../src/parsing.h"

void check_metadata(DArray* metadata_arr)
{
    metadata *tmp_data = (metadata*) DArray_get(metadata_arr, 0);
    relation *rel = tmp_data->data[2];

    for (ssize_t i = 0 ; i < (ssize_t) rel->num_tuples ; i++)
    {
        printf("%lu %lu\n",rel->tuples[i].payload , rel->tuples[i].key);
    }
}

int main() {

    DArray *metadata_arr = DArray_create(sizeof(metadata), 10);

    check(read_relations(metadata_arr) != -1, "Something went wrong in reading the relations");

    do {
        DArray *query_list = parser();
        
        if (DArray_count(query_list) == 0) {
            break;
        }
        
        execute_queries(query_list, metadata_arr);

        for (size_t i = 0 ; i < DArray_count(query_list); i++) {
            query *qr = (query *) DArray_get(query_list, i);
            if (qr != NULL){
                FREE(qr->relations);
                for(size_t j = 0 ; j < qr->predicates_size ; j++)
                    FREE(qr->predicates[j].second);
                FREE(qr->predicates);
                FREE(qr->selects);
            }
        }
        DArray_destroy(query_list);
    } while (0);

    for (size_t i = 0 ; i < DArray_count(metadata_arr); i++) {
        metadata *met = (metadata *) DArray_get(metadata_arr, i);
        for(size_t j = 0 ; j < met->columns ; j++) {
            FREE(met->data[j]->tuples);
            FREE(met->data[j]);
        }
        FREE(met->data);
    }
    
    DArray_destroy(metadata_arr);
    
    return EXIT_SUCCESS;

     error:
        //DArray_destroy(metadata_arr);
        return EXIT_FAILURE;
}
