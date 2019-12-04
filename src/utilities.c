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

        // debug("printing arrays for debugging");

        // metadata *tmp_data = (metadata *) DArray_get(metadata_arr, 0);
        
        //  for (size_t j = 0 ; j < tmp_data->columns ; j++) {
        //      for (size_t i = 0 ; i < tmp_data->tuples ; i++) {
        //          debug("%lu", tmp_data->data[j]->tuples[i].key);
        //      }
        //      debug("");
        //  }

        munmap(mapped_file, sb.st_size);
        close(fd);
    }

    FREE(linptr);
    return 0;

    error:
        FREE(linptr);
        return -1;
}

void parse_relations(char *string, query *q) {
    //find how many relations appear according to spaces
    int spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) 
        if (string[i] == ' ') spaces++;
    
    //allocate memory
    int* relations = (int*) malloc((spaces + 1) * sizeof(int));

    //parse 
    char current_relation[16];
    char* ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%15[^ ]%n", current_relation, &advance) == 1) {
        ptr += advance;
        relations[i++] = (int) strtol(current_relation, NULL, 10);
        if (*ptr != ' ') break;
        ptr++;
    }
    
    q->relations = relations;
    q->relations_size = spaces + 1;
}

void parse_predicates(char *string, query *q) {
    int ampersands = 0;
    for (size_t i = 0; string[i] != '\0'; i++) 
        if (string[i] == '&') ampersands++;
    

    predicate *predicates = (predicate*) malloc((ampersands + 1) * sizeof(predicate));
    
    char current_predicate[128];

    int v1,v2,v3,v4;
    char operator;
    char *ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%127[^&]%n", current_predicate, &advance) == 1) {
        ptr += advance;

        if (sscanf(current_predicate, "%d.%d%c%d.%d", &v1, &v2, &operator, &v3, &v4) == 5){
            relation_column *first = (relation_column*) malloc(sizeof(relation_column));
            first->relation = v1;
            first->column = v2;

            relation_column *second = (relation_column*) malloc(sizeof(relation_column));
            second->relation = v3;
            second->column = v4;

            predicates[i].type = 0;
            predicates[i].first = first;
            predicates[i].second = second;
            predicates[i++].operator = operator;

        } else if (sscanf(current_predicate, "%d.%d%c%d", &v1, &v2, &operator, &v3) == 4) {
            relation_column *first = (relation_column*) malloc(sizeof(relation_column));
            first->relation = v1;
            first->column = v2;

            int *second = (int*) malloc(sizeof(int));
            *second = v3;

            predicates[i].type = 1;
            predicates[i].first = first;
            predicates[i].second = second;
            predicates[i++].operator = operator;
        }
        if (*ptr != '&') break;
        ptr++;
    }
    

    q->predicates = predicates;
    q->predicates_size = ampersands + 1;
    
}

void parse_select(char* string, query *q) {
    //find how many relations appear according to spaces
    int spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) 
        if (string[i] == ' ') spaces++;

    relation_column* selects = (relation_column*) malloc((spaces + 1) * sizeof(relation_column));

    char temp_select[128];
    int relation, column;
    char *ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%127[^ ]%n", temp_select, &advance) == 1) {
        ptr += advance;
        sscanf(temp_select, "%d.%d", &relation, &column);
        selects[i].relation = relation;
        selects[i++].column = column;
        if (*ptr != ' ') break;
        ptr++;
    }

    q->select = selects;
    q->select_size = spaces + 1;
}

DArray* parser(){
    DArray* queries = DArray_create(sizeof(query), 10);

    char* line_ptr = NULL;
    size_t n = 0;
    int characters;
    while ((characters = getline(&line_ptr, &n, stdin)) != -1){
        if (strchr(line_ptr, 'F')) break;
        char relations[128], predicates[128], select[128];
        sscanf(line_ptr, "%[0-9 ]%*[|]%[0-9.=<>&]%*[|]%[0-9. ]", relations, predicates, select);

        query new_query;
        parse_relations(relations, &new_query);
        parse_predicates(predicates, &new_query);
        parse_select(select, &new_query);
        DArray_push(queries, &new_query);

    }
    if (characters == -1) {
        return NULL;
    }
    return queries;
}


void print_relations(int* relations, size_t size){
    printf("Relations: \n\t");
    for (size_t i = 0; i < size; i++) {
        printf("%d ", relations[i]);
    }
    printf("\n");
}

void print_predicates(predicate* predicates, size_t size) {
    printf("Predicates: \n");
    for (size_t i = 0; i < size; i++) {
        if (predicates[i].type == 0) {
            relation_column *temp = (relation_column*) predicates[i].second;
            printf("\t%d.%d %c %d.%d\n", predicates[i].first->relation, predicates[i].first->column, predicates[i].operator
                    , temp->relation, temp->column);
        } else if (predicates[i].type == 1) {
            int *temp = (int*) predicates[i].second;
            printf("\t%d.%d %c %d\n", predicates[i].first->relation, predicates[i].first->column, predicates[i].operator
                    , *temp);
        }
    }
}

void print_select(relation_column* r_c, size_t size){
    printf("Select: \n");
    for (size_t i = 0; i < size; i++){
        printf("\t%d.%d ", r_c[i].relation, r_c[i].column);
    }
    printf("\n");
    
}

void execute_filter(predicate pred , int* relations , int relations_size ,DArray *metadata_arr , mid_results** mid_results_arr)
{
    size_t tuples =0;    
    mid_results *tmp_results=NULL ;

    //check if the relation exists in the middle results
    int rel_exists = -1;
    for(size_t i = 0 ; i < (size_t)relations_size ; i++){
        if ( ( mid_results_arr[i] != NULL ) && ( mid_results_arr[i]->relation == pred.first->relation) ) {
            rel_exists = i;
            break;
        }
    }

    metadata *tmp_data = (metadata*) DArray_get(metadata_arr, pred.first->relation);
    relation* rel = tmp_data->data[pred.first->column];
    uint64_t *number = (uint64_t*) pred.second; 

    if (rel_exists >= 0 ) //if the relation exists in the middle results
    {   //printf("REL EXISTS = %d\n",rel_exists);

        //take the saved payloads and check if they satisfy the new filter
        for(size_t i = 0 ; i <  DArray_count(mid_results_arr[rel_exists]->payloads) ; i++)
        {   
            uint64_t* payload = (uint64_t*) DArray_get(mid_results_arr[rel_exists]->payloads,i);
            
            //every payload that does not satisfy the new filter remove it from the dynamic array 
            if ( pred.operator == '='){
                if ( rel->tuples[*payload].key != *number ){
                    DArray_remove(mid_results_arr[rel_exists]->payloads,i);
                    i--; //we removed an item so the i should not increase in this loop
                }
            }
            else if ( pred.operator == '>'){
                if ( rel->tuples[*payload].key <= *number ){
                    DArray_remove(mid_results_arr[rel_exists]->payloads,i);
                    i--;
                }
            }
             else if ( pred.operator == '<'){ 
                if ( rel->tuples[*payload].key >= *number ){   
                    DArray_remove(mid_results_arr[rel_exists]->payloads,i);
                    i--;
                }
            }
            else{   
                printf("Wrong operator \n");
                return ;
            }
        }
    }
    else if (rel_exists < 0) // if the relation does not exist in the middle results
    {   
        //set up and fill the struct of this relation in the middle results
        tmp_results = MALLOC(mid_results,1);
        tmp_results->relation = relations[pred.first->relation];
        tmp_results->payloads = DArray_create(sizeof(uint64_t), 100);
        
        //place this struct in the first available cell of the middle results
        for(size_t i = 0 ; i < (size_t) relations_size ; i++){
            if  ( mid_results_arr[i] == NULL ) {
                mid_results_arr[i] = tmp_results;
                break;
            }
        }

        tuples = rel->num_tuples;
        //check every tuple of the relation if it satisfies the filter 
        for (size_t i = 0 ; i < tuples ; i++){
            
            //if the tuple satisfies the filter push it in the dynamic array of the payloads
            if ( pred.operator == '='){
                if ( rel->tuples[i].key == *number ){
                    DArray_push(tmp_results->payloads , &(rel->tuples[i].payload));
                }
            }
            else if ( pred.operator == '>'){
                if ( rel->tuples[i].key > *number ){
                    // printf("%lu %lu\n", rel->tuples[i].payload , rel->tuples[i].key);
                    // if (rel->tuples[i].key < 6000)
                    //     counter++;
                    DArray_push(tmp_results->payloads ,  &(rel->tuples[i].payload));
                }
            }
            else if ( pred.operator == '<'){
                if ( rel->tuples[i].key < *number ){   
                    DArray_push(tmp_results->payloads , &( rel->tuples[i].payload));
                }
            }
            else{   
                printf("Wrong operator \n");
                return ;
            }
        }        
    
    }
    printf("END FILTER %d \n",counter);

}

void execute_query(query* q , DArray* metadata_arr)
{
    printf("Execute Queries\n");
    //first execute filter predicates 

    mid_results** mid_results_arr = MALLOC(mid_results*,q->relations_size);
    for(size_t i = 0 ; i < (size_t) q->relations_size ; i++ ) //initialize middle results array
        mid_results_arr[i] = NULL;


    for(size_t i = 0 ; i < (size_t)q->predicates_size ; i++){
        
        if( q->predicates[i].type == 1){ //filter predicate
            execute_filter( q->predicates[i] , q->relations , q->relations_size , metadata_arr , mid_results_arr);
        }
    }

    for(size_t i = 0 ; i < (size_t) q->relations_size ; i++ ){
        if (mid_results_arr[i] != NULL){
            DArray_destroy( mid_results_arr[i]->payloads);
            FREE(mid_results_arr[i]);
        }
    }

    FREE(mid_results_arr);	
}
