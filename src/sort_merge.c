#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include <time.h>
#include "alloc_free.h"
#include "structs.h"
#include "result_list.h"
#include "dbg.h"
#include "utilities.h"
#include "queue.h"
#include "quicksort.h"


void recursive_sort(relation *relR, relation *reorderedR, uint8_t byte, int start, int size) {
   
    if (byte > 8)
        return;

    histogram hist, psum;
    build_histogram(relR, &hist, byte, start, size);
    build_psum(&hist, &psum);
    reorderedR = build_reordered_array(reorderedR, relR, &hist, &psum, byte, start, size);

    for (size_t i = 0 ; i < 256 ; i++) {
        if (hist.array[i]*sizeof(uint64_t) < 64*1024) {
            random_quicksort(reorderedR->tuples, start + psum.array[i], start + psum.array[i] + hist.array[i]);
        }
        else if (hist.array[i] != 0 && byte != 9) 
            recursive_sort(reorderedR, relR, byte + 1, start + psum.array[i], hist.array[i]); //to byte++ pou xes me kseskise xthes
        
    }
}

result *join_relations(relation *relR, relation *relS) {

    result *res_list = create_result_list();
    int mark = -1;
    size_t pr = 0;
    size_t ps = 0;

    size_t count = 0;

    while (pr < relR->num_tuples && ps < relS->num_tuples) {
        if (mark == -1) {
            while (relR->tuples[pr].key < relS->tuples[ps].key) {
                pr++;
            }
            while (relR->tuples[pr].key > relS->tuples[ps].key) {
                ps++;
            }
            mark = ps;
        }
        if (relR->tuples[pr].key == relS->tuples[ps].key) {
            count++;
            check(add_to_result_list(res_list, relR->tuples[pr].payload, relS->tuples[ps].payload) != -1, "Couldn't add to result list!");
            ps++;
        } else {
            ps = mark;
            pr++;
            mark = -1;
        }
    }

    log_info("Found %lu records", count);
   
    return res_list;

    error:
        return NULL;
}

int iterative_sort(relation *rel) {
    
    relation *reordered = allocate_reordered_array(rel);
    queue *q = new_queue();
    check_mem(q);

    relation *relations[2];

    histogram new_h, new_p;
    build_histogram(rel, &new_h, 1, 0, rel->num_tuples);
    build_psum(&new_h, &new_p);
    reordered = build_reordered_array(reordered, rel, &new_h, &new_p, 1, 0, rel->num_tuples);
    
    relations[0] = rel;
    relations[1] = reordered;

    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0)
            push(q, new_item(new_p.array[j], new_h.array[j]));
                 
    }
    ssize_t i;
    for (i = 2; i < 9; i++) {
        int number_of_buckets = size(q);

        while (number_of_buckets) {

            item* current_item = pop(q);
            ssize_t base = current_item->base, size = current_item->size;
            delete_item(current_item);
            number_of_buckets--;

            if (size*sizeof(uint64_t) < 64*1024) {
                random_quicksort(relations[(i+1)%2]->tuples, base, base +size-1);
                for (ssize_t j = base; j < base + size; j++) {

                    relations[i%2]->tuples[j] = relations[(i+1)%2]->tuples[j];
                }
            } else {
                histogram new_h, new_p;
                build_histogram(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
            
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0)
                        push(q, new_item(base + new_p.array[j], new_h.array[j]));
                }
            }
        }
    }

    free_reordered_array(reordered);
    delete_queue(q);
    return 0;

    error:
        free_reordered_array(reordered);
        delete_queue(q);
        return -1;
}

result* SortMergeJoin(relation *relR, relation *relS) {

    check(iterative_sort(relR) != -1, "Couldn't sort the relation, something came up");
    
    check(iterative_sort(relS) != -1, "Couldn't sort the relation, something came up");

    if (relR->num_tuples < relS->num_tuples)
        return join_relations(relR , relS);
    else
        return join_relations(relS , relR);
    

    error:
        return NULL;
}