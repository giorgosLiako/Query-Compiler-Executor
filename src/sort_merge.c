
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <sys/types.h>
#include "alloc_free.h"
#include "DArray.h"
#include "structs.h"
#include "result_list.h"
#include "dbg.h"
#include "Queue_interface.h"

#define get_byte(num, byte) ( num >> ( (sizeof(num) << 3) - (byte << 3) ) & 0xFF)

void build_histogram(relation *rel, histogram *hist, int wanted_byte, int start, int size) {

    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }

    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->array[byte]++;
    }
}

void build_psum(histogram *hist, histogram *psum) {

    for (size_t i =  0 ; i < 256 ; i++) {
        psum->array[i] = -1;
    }

    size_t offset = 0;
    for (ssize_t i = 0;  i < 256 ; i++) {
        if (hist->array[i] != 0) {
            psum->array[i] = offset;
            offset +=  hist->array[i];
        }
    }

}


relation* build_reordered_array(relation* reorder_rel , relation *prev_rel, 
                                histogram* histo , histogram* psum, 
                                int wanted_byte, int start, int size) {
   
    histogram temp = *histo;

    
    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t index = start +  psum->array[byte] + (histo->array[byte] - temp.array[byte]);

        temp.array[byte]--;    

        reorder_rel->tuples[index].key = prev_rel->tuples[i].key;
        reorder_rel->tuples[index].payload = prev_rel->tuples[i].payload;
    }

    return reorder_rel;
}


relation* allocate_reordered_array(relation* rel) {
    
    relation *r = NULL;
    r = MALLOC(relation, 1);
    check_mem(r);

    r->num_tuples = rel->num_tuples;
    r->tuples = MALLOC(tuple, rel->num_tuples);
    check_mem(r->tuples);

    return r;

    error:
        return NULL;
}

void free_reordered_array(relation* r) {
    FREE(r->tuples);
    FREE(r);
}

ssize_t random_in_range(ssize_t low, ssize_t high) {

    ssize_t r = rand();

    r = (r << 31) | rand();

    return r % (high - low + 1) + low;
}

void swap(tuple *tuples, ssize_t i, ssize_t j) {
    tuple tup = tuples[i];
    tuples[i] = tuples[j];
    tuples[j] = tup;
}


ssize_t hoare_partition(tuple *tuples, ssize_t low, ssize_t high) {

    ssize_t i = low - 1;
    ssize_t j = high + 1;

    ssize_t random = random_in_range(low, high);

    swap(tuples, low, random);

    uint64_t pivot = tuples[high].key;

    while (1) {

        do {
            i++;
        } while (tuples[i].key < pivot);

        do {
            j--;
        } while (tuples[j].key > pivot);

        if (i >= j) {
            return j;
        }

        swap(tuples, i, j);
    }
}

void random_quicksort(tuple *tuples, ssize_t low, ssize_t high) {

    if (low >= high) {
        return;
    }

    ssize_t pivot = hoare_partition(tuples, low, high);

    random_quicksort(tuples, low, pivot);
    random_quicksort(tuples, pivot + 1, high);
}

// void is_sorted(relation * rel) {
//     for (size_t i = 1 ; i < rel->num_tuples ; i++ )
//     {
//         if ( rel->tuples[i].key < rel->tuples[i-1].key )
//         {
//             printf("rel[%ld] = %ld rel[%ld] = %ld \n", i-1 , rel->tuples[i-1].key , i , rel->tuples[i].key);
//             printf("NOT SORTED\n");
//             return;
//         }
//     }

//     printf("SORTED\n");
// }

void recursive_sort(relation *relR, relation *reorderedR, int byte, int start, int size) {
   
    if (byte > 8)
        return;

    histogram hist, psum;
    build_histogram(relR, &hist, byte, start, size);
    build_psum(&hist, &psum);
    reorderedR = build_reordered_array(reorderedR, relR, &hist, &psum, byte, start, size);

    debug("byte = %d, start = %d, size = %d", byte, start, size);

    for (size_t i = 0 ; i < 256 ; i++) {
        if (hist.array[i] != 0 && byte != 9) {
            recursive_sort(reorderedR, relR, byte + 1, start + psum.array[i], hist.array[i]); //to byte++ pou xes me kseskise xthes
        } 
        else {
         //   random_quicksort(reorderedR->tuples, start + psum.array[i], start + psum.array[i] + hist.array[i]);
        }
    }
}

// relation* iterative_sort(DArray *stack, relation *relR, relation *reorderedR) {

//     histogram *hist, *psum;
  
//     hist = MALLOC(histogram, 1);
//     check_mem(hist);
//     psum = MALLOC(histogram, 1);
//     check_mem(psum);

//     stack_node s_node, temp;
//     s_node.hist = hist;
//     s_node.psum = psum;
//     s_node.current_byte = 1;
//     s_node.reorderedR = reorderedR;
//     s_node.relR = relR;
//     s_node.start = 0;
//     s_node.size = relR->num_tuples;

//     DArray_push(stack, &s_node);

//     bool swap_relations = true;
//     while (1) {

//         void *element = DArray_pop(stack);
//         s_node = *(stack_node *) element;

//         build_histogram(s_node.relR, s_node.hist, s_node.current_byte, s_node.start, s_node.size);
//         build_psum(s_node.hist, s_node.psum);
//         s_node.reorderedR = build_reordered_array(s_node.reorderedR, s_node.relR, s_node.hist, s_node.psum, s_node.current_byte, s_node.start, s_node.size);
        
//         debug("byte = %d, start = %d, size = %d", s_node.current_byte, s_node.start, s_node.size);

//         for (size_t i = 0 ; i < 256 ; i++) {
            
//             if (s_node.hist->array[i] != 0 && s_node.current_byte != 9) {
//                 /*Create new snapshot */
//                 s_node.current_byte++;
//                 s_node.start += s_node.psum->array[i];
//                 s_node.size = s_node.hist->array[i];
               
//                 if (swap_relations) {                      
//                     relation *temp = s_node.relR;
//                     s_node.relR = s_node.reorderedR;
//                     s_node.reorderedR = temp;

//                     swap_relations = false;
//                 } else {
//                     relation *temp = s_node.reorderedR;
//                     s_node.reorderedR = s_node.relR;
//                     s_node.relR = temp;

//                     swap_relations = true;
//                 }

//                 DArray_push(stack, &s_node);
//             } 
//             else {
//                 //random_quicksort(s_node.reorderedR->tuples, s_node.start + s_node.psum->array[i], s_node.start + s_node.psum->array[i] + s_node.hist->array[i]);
//             }
//         }
//         if (!DArray_count(stack)) {
//             relation *retval = s_node.relR;
//             FREE(element);
//             return retval;
//         }
//         FREE(element);
//     }

//     error:
//         return NULL;
// }

bool is_sorted(relation *relR, uint64_t num_tuples) {

    for (size_t i = 1 ; i < num_tuples ; i++) {
        if (relR->tuples[i].key < relR->tuples[i - 1].key) {
            return false;
        }
    }

    return true;
}

void alternative_without_recursion(relation *);
void Join_relations(relation* , relation* );

result* SortMergeJoin(relation *relR, relation *relS) {

    relation *reorderedR = NULL;
    relation *reorderedS = NULL; 
    
    reorderedR = allocate_reordered_array(relR);
    reorderedS = allocate_reordered_array(relS);

    //recursive_sort(relR, reorderedR, 1, 0, relR->num_tuples);

    //DArray *stack = DArray_create(sizeof(stack_node), 25);
    
    //relR = iterative_sort(stack, relR, reorderedR);

    // for (size_t i = 0; i < relR->num_tuples; i++) {
    //         printf("%ld\t%lu\n", relR->tuples[i].key, relR->tuples[i].payload);
    // }

    // if (is_sorted(relR, relR->num_tuples)) {
    //     printf("Its sorted\n");
    // }

    alternative_without_recursion(relR);
     if (is_sorted(relR, relR->num_tuples))
         printf("R is sorted\n");

    alternative_without_recursion(relS);
     if (is_sorted(relS, relS->num_tuples))
         printf("S is sorted\n");


    //Join(relR , relS);


    free_reordered_array(reorderedR);
    free_reordered_array(reorderedS);
    //DArray_destroy(stack);

    return NULL;
}





    // stack_node s_node;

    // DArray *stack = DArray_create(sizeof(stack_node), 25);

    // while (1) {

    //     s_node.hist = MALLOC(histogram, 1);
    //     check_mem(s_node.hist);
    //     s_node.psum = MALLOC(histogram, 1);
    //     check_mem(s_node.psum);

    //     DArray_push(stack, &s_node);

    //     build_histogram(relR, s_node.hist, byte);
    //     build_psum(s_node.hist, s_node.psum);

    //     reorderedR = build_reordered_array(reorderedR, relR , s_node.hist, s_node.psum, byte);
    // }

    // error:
    //     free_reordered_array(reorderedR);
    //     FREE(s_node.hist);
    //     FREE(s_node.psum);
    //     return NULL; 




void swap_arrays(relation* r1, relation* r2) {
    relation* temp;
    temp = r1;
    r1 = r2;
    r2 = temp;
}

void alternative_without_recursion(relation *rel) {
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
        int number_of_items = size(q);

        while (number_of_items) {

            item* current_item = pop(q);
            ssize_t base = current_item->base, size = current_item->size;


            histogram new_h, new_p;
            build_histogram(relations[(i+1)%2], &new_h, i, base, size);
            build_psum(&new_h, &new_p);
            relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
  
        
            for (ssize_t j = 0; j < 256 && i != 8; j++) {
                if (new_h.array[j] != 0){
                    push(q, new_item(base + new_p.array[j], new_h.array[j]));
                 }
            }
            delete_item(current_item);


            number_of_items--;
        }
        
        
    }
/*
    for (size_t j = 0; j < relations[(i+1)%2]->num_tuples; j++)
    {
        printf("%ld %ld\n", relations[(i+1)%2]->tuples[j].key, relations[(i+1)%2]->tuples[j].payload);
    }
    printf("%d\n", size(q));
*/
    error:
        if (reordered != NULL) free_reordered_array(reordered);
        if (q != NULL) delete_queue(q);
        return;
}