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
#include "structs.h"

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

relation_info* relation_exists(DArray *mid_results, uint32_t relation) {

    for (ssize_t i = 0 ; i < DArray_count(mid_results) ; i++) {
        mid_result *res = (mid_result *) DArray_get(mid_results, i);
        for (ssize_t j = 0 ; j < DArray_count(res->rel_infos) ; j++) {
            relation_info *rel_info = DArray_get(res->rel_infos, j);
            if (rel_info->relation == relation) {
                return rel_info;
            }
        }
    }
    return NULL;
}
   

static void print_sums(DArray *mid_results, uint32_t *relations, DArray *metadata_arr, relation_column *selects, size_t select_size) {

    for (size_t i = 0 ; i < select_size ; i++) {
        uint32_t rel = relations[selects[i].relation];
        uint32_t col = selects[i].column;

        ssize_t index = relation_exists(mid_results, rel);
        if (index == -1) {
            log_err("Something went really wrong...");
            exit(EXIT_FAILURE);
        }
        mid_result *res = DArray_get(mid_results, index);

        if (DArray_count(res->payloads) == 0) {
            printf("%s", "NULL\n");
        } else {
            uint64_t sum = 0;
            relation *tmp_rel = ((metadata *) DArray_get(metadata_arr, rel))->data[col];
            for (ssize_t j = 0 ; j < DArray_count(res->payloads) ; j++) {
                sum += tmp_rel->tuples[*(uint64_t *) DArray_get(res->payloads, j)].key;
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

static int execute_query(query* q , DArray* metadata_arr) {

    DArray *mid_results = DArray_create(sizeof(mid_result), 2);

    debug("Executing query : ");
    print_relations(q->relations, q->relations_size);
    print_predicates(q->predicates, q->predicates_size);
    print_select(q->selects, q->select_size);
    
  
    for (size_t i = 0 ; i < (size_t)q->predicates_size ; i++) {

        if( q->predicates[i].type == 1) { //filter predicate
            check(execute_filter(&q->predicates[i], q->relations, metadata_arr, mid_results) != -1, "");
        } else {    //join predicate
           check(execute_join(&q->predicates[i], q->relations, metadata_arr, mid_results) != -1, "Join failed!");
        }
    }

    print_sums(mid_results, q->relations, metadata_arr, q->selects, q->select_size);

    for (size_t i = 0 ; i < DArray_count(mid_results) ; i++) {
        mid_result*res = (mid_result *) DArray_get(mid_results, i);

        if (res != NULL) {
            DArray_destroy(res->payloads);
        }
    }    
    DArray_destroy(mid_results);

    return 0;

    error:
        return -1;
}

mid_result* create_entity(DArray *mid_results) {
    
    mid_result res;
    res.rel_infos = DArray_create(sizeof(relation_info), 3);

    DArray_push(mid_results, &res);

    return (mid_result *) DArray_last(mid_results);
}

relation_info* add_to_entity(mid_result *res, uint32_t relation) {

    relation_info rel_info;
    rel_info.last_col_sorted = -1;
    rel_info.relation = relation;
    DArray_push(res, &rel_info);

    return (relation_info *) DArray_last(res);
}

void execute_queries(DArray *q_list, DArray *metadata_arr) {


    for (size_t i = 0; i < DArray_count(q_list); i++) {

        query *tmp_data = (query*) DArray_get(q_list, i);

        arrange_predicates(tmp_data);

        execute_query(tmp_data , metadata_arr);
    }
}
