#include "quicksort.h"

//function to produce a random number between low and high
static ssize_t random_in_range(ssize_t low, ssize_t high) {

    ssize_t r = rand();

    r = (r << 31) | rand();

    return r % (high - low + 1) + low;
}

//function to swap two tuples 
static void swap(tuple *tuples, ssize_t i, ssize_t j) {
    tuple tup = tuples[i];
    tuples[i] = tuples[j];
    tuples[j] = tup;
}

//function to define the partition
static ssize_t hoare_partition(tuple *tuples, ssize_t low, ssize_t high) {

    ssize_t i = low - 1;
    ssize_t j = high + 1;

    ssize_t random = random_in_range(low, high);//pick a random number

    swap(tuples, low, random); //swap tuple in index low with tuple in index random

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

//function to execute a quicksort
static void random_quicksort(tuple *tuples, ssize_t low, ssize_t high) {

    if (low >= high) {
        return;
    }

    ssize_t pivot = hoare_partition(tuples, low, high);

    random_quicksort(tuples, low, pivot);
    random_quicksort(tuples, pivot + 1, high);
}

static void *sort_job(void *argm) {

    sort_args args = *(sort_args *) argm;

    DArray *current_queue = args.queue;
    uint32_t start = args.start;
    uint32_t end = args.end;
    int index = args.index;
    relation *relations[2] = {args.relations[0], args.relations[1]};

    DArray *temp_queue = NULL;
    bool swapped = false;
    for (ssize_t i = index ; i < 9 ; i++) {
        
        if (!(end > 0)) {
            break;
        }

        temp_queue = DArray_create(sizeof(queue_node), 2000);
        for (ssize_t z = start ; z < end ; z++) {
    
            queue_node *current_item = (queue_node *) DArray_get(current_queue, z);
            ssize_t base = current_item->base, size = current_item->size; 
    
            if ( size*sizeof(tuple) + sizeof(uint64_t) < 32*1024) {
                random_quicksort(relations[(i+1)%2]->tuples, base, base + size - 1);
                for (ssize_t j = base; j < base + size; j++) {
                    relations[i%2]->tuples[j] = relations[(i+1)%2]->tuples[j];
                }

            } else { 
                histogram new_h, new_p;
                build_histogram(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0) {
                        queue_node q_node;
                        q_node.base = base + new_p.array[j];
                        q_node.size = new_h.array[j];
                        DArray_push(temp_queue, &q_node);
                    }
                }
            }
        }
        if (swapped) {
            DArray_destroy(current_queue);
        }
        current_queue = temp_queue;
        swapped = true;
        start = 0;
        end = DArray_count(temp_queue);
    }
    if (swapped) {
        DArray_destroy(temp_queue);
    }
    return NULL;
}

//functions that sorts iterative one relation
int iterative_sort(relation *rel, DArray **retval, thr_pool_t *pool) {
    
    relation *reordered = allocate_reordered_array(rel);
    DArray *q = DArray_create(sizeof(queue_node), 256);
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
    DArray *q_to_return = DArray_create(sizeof(queue_node), 256);
    bool check_queue = true;
    for (ssize_t j = 0; j < 256; j++) {
        if (new_h.array[j] != 0) {
            queue_node q_node;
            q_node.byte = j;
            q_node.base = new_p.array[j];
            q_node.size = new_h.array[j];
            DArray_push(q, &q_node);
        }
    }

    ssize_t i;
    int number_of_buckets = 0;
    //above we execute the routine for the first byte
    //now for all the other bytes
    for (i = 2; i <= 8 ; i++) {
        
        number_of_buckets = DArray_count(q);
        if (check_queue && number_of_buckets >= 2) {
            for (ssize_t tmp_i = 0 ; tmp_i < DArray_count(q) ; tmp_i++) {
                queue_node q_node = *(queue_node *) DArray_get(q, tmp_i);
                DArray_push(q_to_return, &q_node);
            }
            check_queue = false;
        }
        if (!(number_of_buckets > 0) || (number_of_buckets / MAX_JOBS) > 0) {
            break;
        }

        //the size of the queue is the number of the buckets
        while (number_of_buckets) { //for each bucket
    
            queue_node *current_item = DArray_get(q, 0);
            ssize_t base = current_item->base, size = current_item->size; // base = start of bucket , size = the number of cells of the bucket
            DArray_remove(q, 0);
          
            number_of_buckets--;
            //check if the bucket is smaller than 32 KB to execute quicksort
            if ( size*sizeof(tuple) + sizeof(uint64_t) < 32*1024) {
                random_quicksort(relations[(i+1)%2]->tuples, base, base + size - 1);
                for (ssize_t j = base; j < base + size; j++) {

                    relations[i%2]->tuples[j] = relations[(i+1)%2]->tuples[j];
                }

            } else { // if the bucket is bigger than 32 KB , sort by the next byte
                histogram new_h, new_p;
                //build again histogram and psum of the next byte
                build_histogram(relations[(i+1)%2], &new_h, i, base, size);
                build_psum(&new_h, &new_p);
                //build the reordered array of the previous bucket
                relations[i%2] = build_reordered_array(relations[i%2], relations[(i+1)%2], &new_h, &new_p, i, base, size);
    
                //push the buckets to the queue
                for (ssize_t j = 0; j < 256 && i != 8; j++) {
                    if (new_h.array[j] != 0) {
                        queue_node q_node;
                        q_node.byte = j;
                        q_node.base = base + new_p.array[j];
                        q_node.size = new_h.array[j];
                        DArray_push(q, &q_node);
                    }
                }
            }
        }
    }
    number_of_buckets = DArray_count(q);
    //If it wasn't yet sorted, try multithreading
    if ((number_of_buckets / MAX_JOBS) > 0) {
        
        uint32_t jobs_to_create = MAX_JOBS;

        sort_args *sort_job_args = MALLOC(sort_args, jobs_to_create);
        check_mem(sort_job_args);

        for (ssize_t j = 0 ; j < (ssize_t) jobs_to_create ; j++) {
            sort_args argm;
            argm.queue = q;
            argm.start = j * (number_of_buckets / jobs_to_create);
            argm.end = (j + 1) * (number_of_buckets / jobs_to_create);
            if (j == jobs_to_create - 1) {
                argm.end += number_of_buckets % jobs_to_create;
            }
            argm.index = i;
            argm.relations[0] = relations[0];
            argm.relations[1] = relations[1];

            sort_job_args[j] = argm;

            thr_pool_queue(pool, sort_job, (void *) &sort_job_args[j]);
        }
        thr_pool_barrier(pool);        
    }
    *retval = q_to_return;

    free_reordered_array(reordered);
    DArray_destroy(q);
    return 0;

    error:
        free_reordered_array(reordered);
        DArray_destroy(q);
        return -1;
}