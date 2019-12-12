#define _GNU_SOURCE
#include <stdio.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "utilities.h"
#include "histogram.h"
#include "DArray.h"
#include "pred_arrange.h"
#include "join.h"
#include "filter.h"

//function to build a histogram
void build_histogram(relation *rel, histogram *hist, uint8_t wanted_byte, int start, int size) {
    //start is the index that starts the bucket and size is the number of tuples that has
    //intialize the histogram
    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }
    //take the wanted byte of the tuples that belong to the bucket and make the histogram
    for (ssize_t i = start ; i < start + size ; i++) {
        uint32_t byte = get_byte(rel->tuples[i].key, wanted_byte);
        hist->array[byte]++; 
    }
}

//function to build psum
void build_psum(histogram *hist, histogram *psum) {
    //initialize the psum
    for (size_t i =  0 ; i < 256 ; i++) {
        psum->array[i] = -1;
    }

    size_t offset = 0;
    for (ssize_t i = 0;  i < 256 ; i++) {
        if (hist->array[i] != 0) { //every cell is not 0 there is in the histogram , so put it in the psum
            psum->array[i] = offset; 
            offset +=  hist->array[i]; //find the offset with the help of histogram
        }
    }

}

//function to build the reordered array
relation* build_reordered_array(relation* reorder_rel , relation *prev_rel, 
                                histogram* histo , histogram* psum, 
                                uint8_t wanted_byte, int start, int size) {
   
    histogram temp = *histo;

    
    for (ssize_t i = start ; i < start + size ; i++) {
        uint8_t byte = get_byte(prev_rel->tuples[i].key, wanted_byte);

        size_t index = start +  psum->array[byte] + (histo->array[byte] - temp.array[byte]);

        temp.array[byte]--;    

        reorder_rel->tuples[index].key = prev_rel->tuples[i].key;
        reorder_rel->tuples[index].payload = prev_rel->tuples[i].payload;
    }

    return reorder_rel;
}


//function to allocate the memory for the reordered array
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

//function to free the allocated memory of the reordered array
void free_reordered_array(relation* r) {
    FREE(r->tuples);
    FREE(r);
}

//function to swap arrays
void swap_arrays(relation* r1, relation* r2) {
    relation* temp;
    temp = r1;
    r1 = r2;
    r2 = temp;
}


static inline void fill_data(metadata *m_data, uint64_t *mapped_file) {
    
    m_data->tuples = *mapped_file++;
    m_data->columns = *mapped_file++;
    m_data->data = MALLOC(relation *, m_data->columns);

    for (size_t j = 0 ; j < m_data->columns ; j++) {
        relation *rel = MALLOC(relation, 1);
        rel->num_tuples = m_data->tuples;
        rel->tuples = MALLOC(tuple, m_data->tuples);
        m_data->data[j] = rel;
        for (size_t i = 0 ; i < m_data->tuples ; i++) {
            rel->tuples[i].key = *mapped_file++;
            rel->tuples[i].payload = i;
        }
    }
}


int read_relations(DArray *metadata_arr) {

    char *linptr = NULL;
    size_t n = 0;

    printf("%s\n", "Insert relations");
    while (getline(&linptr, &n, stdin) != -1) {
        if (!strncmp(linptr, "Done\n", strlen("Done\n")) || !strncmp(linptr,"done\n", strlen("done\n"))) {
            break;
        }
        
        linptr[strlen(linptr) - 1] = '\0';
        int fd = open(linptr, O_RDONLY, S_IRUSR | S_IWUSR);
        check(fd != -1, "open failed");

        struct stat sb;

        check(fstat(fd, &sb) != -1, "fstat failed");

        uint64_t *mapped_file = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        check(mapped_file != MAP_FAILED, "mmap failed");

        metadata m_data;

        fill_data(&m_data, mapped_file);

        DArray_push(metadata_arr, &m_data);

        munmap(mapped_file, sb.st_size);
        close(fd);
    }

    FREE(linptr);
    return 0;

    error:
        FREE(linptr);
        return -1;
}

ssize_t relation_exists(DArray *mid_results, uint32_t relation) {

    ssize_t found = -1;
    for (ssize_t i = 0 ; i < DArray_count(mid_results) ; i++) {
        mid_result res = *(mid_result *) DArray_get(mid_results, i);
        if (res.relation == relation) {
            found = i;
        }
    }

    if (found == -1) {
        mid_result res;
        res.relation = relation;
        res.last_column_sorted = -1;
        res.payloads = DArray_create(sizeof(uint64_t), 20);
        DArray_push(mid_results, &res);
    }

    return found;
}


void print_predicates(query *qry) {

    for (size_t i = 0 ; i < qry->predicates_size ; i++) {
        predicate current = qry->predicates[i];
        
        if (current.type == 1) {
            uint32_t second = *(uint32_t *) current.second;
            printf("%lu.%lu %c %u|", current.first.relation, current.first.column, current.operator, second);
        }
        else {
            relation_column *rel_col = (relation_column *) current.second;
            printf("%lu.%lu %c %lu.%lu| ", current.first.relation, current.first.column, current.operator, rel_col->relation, rel_col->column);
        }
    }
    printf("\n\n");
}

static int execute_query(query* q , DArray* metadata_arr) {

    DArray *mid_results = DArray_create(sizeof(mid_result), 4);
    
    for (size_t i = 0 ; i < (size_t)q->predicates_size ; i++) {

        if( q->predicates[i].type == 1) { //filter predicate
            check(execute_filter(&q->predicates[i], q->relations[i], metadata_arr, mid_results) != -1, "");
        } else {    //join predicate
           check(execute_join(&q->predicates[i], q->relations, metadata_arr, mid_results) != -1, "Join failed!");
        }
    }	

    return 0;

    error:
        return -1;
}

void execute_queries(DArray *q_list, DArray *metadata_arr) {

    for (size_t i = 0; i < DArray_count(q_list); i++) {

        query *tmp_data = (query*) DArray_get(q_list, i);

        printf("Printing before arrangement\n");
        print_predicates(tmp_data);

        arrange_predicates(tmp_data);

        printf("\nPrinting after arrangement\n");
        print_predicates(tmp_data);
        //execute_query(tmp_data , metadata_arr);
    }
}
