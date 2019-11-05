#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <time.h>
#include <CUnit/CUnit.h>
#include <CUnit/Basic.h>
#include "alloc_free.h"
#include "structs.h"
#include "result_list.h"
#include "dbg.h"
#include "utilities.h"
#include "queue.h"
#include "quicksort.h"

//function to join two relations
result *join_relations(relation *relR, relation *relS) {

    result *res_list = create_result_list(); //initialize the list of results
    int mark = -1;  //mark for duplicates
    size_t pr = 0; //pointer to relation R
    size_t ps = 0; // pointer to relation S

    size_t count = 0; //counter of founded joins

    //loop while we dont reach the end of none array
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
            count++;
            check(add_to_result_list(res_list, relR->tuples[pr].payload, relS->tuples[ps].payload) != -1, "Couldn't add to result list!");
            ps++;
        
        } else { //the keys of the relations are different
            ps = mark;
            pr++;
            mark = -1; //initialize mark
        }
    }

    log_info("Found %lu records", count);
   
    return res_list;

    error:
        return NULL;
}

//functions that sorts iterative one relation
int iterative_sort(relation *rel) {
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

//struct for CUnit
typedef struct sort_test {    
    relation *relR;
    relation *relS;
} sort_test;

sort_test test_var;

//function for CUinit : checks if the array is sorted 
void is_sorted() {

    for (size_t i = 1 ; i < test_var.relR->num_tuples ; i++) {
        CU_ASSERT(test_var.relR->tuples[i].key >= test_var.relR->tuples[i - 1].key)
    }
    for (size_t i = 1 ; i < test_var.relS->num_tuples ; i++) {
        CU_ASSERT(test_var.relS->tuples[i].key >= test_var.relS->tuples[i - 1].key);
    }
}

result* SortMergeJoin(relation *relR, relation *relS) {

    CU_pSuite pSuite = NULL;
    //initialize CU suite
    if (CUE_SUCCESS != CU_initialize_registry()) {
        goto error;
    }

    pSuite = CU_add_suite("Sorting test", 0, 0);
    if (NULL == pSuite) {
        CU_cleanup_registry();
        goto error;
    }
    //check if the relations after iterative sort are sorted
    if (NULL == CU_add_test(pSuite, "Test if both relations are sorted", is_sorted)) {
        CU_cleanup_registry();
        goto error;
    }

    check(iterative_sort(relR) != -1, "Couldn't sort the relation, something came up");
    
    check(iterative_sort(relS) != -1, "Couldn't sort the relation, something came up");

    test_var.relR = relR;
    test_var.relS = relS;
    //run the test
    CU_basic_set_mode(CU_BRM_VERBOSE);
    CU_basic_run_tests();
    CU_cleanup_registry();

    //join the two relations
    //the first argument of function join_Relations must be the relation with the least num_tuples
    if (relR->num_tuples < relS->num_tuples)
        return join_relations(relR , relS);
    else
        return join_relations(relS, relR);

    error:
        return NULL;
}