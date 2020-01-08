#include "utilities.h"

#define N 30001891

void build_histogram_darray(DArray *tuples, histogram *hist, uint8_t wanted_byte, int start, int size) {

    for (size_t i = 0 ; i < 256 ; i++) {
        hist->array[i] = 0;
    }

    for (ssize_t i = start ; i < start + size ; i++) {
        tuple *tup = (tuple *) DArray_get(tuples, i);
        uint32_t byte = get_byte(tup->key, wanted_byte);
        hist->array[byte]++;
    }
}

DArray* build_reordered_darray(DArray *reorder_rel, DArray *prev_rel, histogram *histo, histogram *psum, uint8_t wanted_byte, int start, int size) {

    histogram temp = *histo;

    for (ssize_t i = start ; i < start + size ; i++) {
        tuple *tup = (tuple *) DArray_get(prev_rel, i);
        uin8_t byte = get_byte(tup->key, wanted_byte);

        size_t index = start + psum->array[byte] + (histo->array[byte] - temp.array[byte]);
        temp.array[byte]--;

        tuple to_push;
        to_push.key = tup->key;
        to_push.payload = tup->payload;

        DArray_set(reorder_rel, index, &to_push);
    }

    return reorder_rel;
}

DArray* allocate_reordered_darray(DArray *rel) {
   
    DArray *tmp = DArray_create(sizeof(tuple), DArray_count(rel));
    check_mem(tmp);

    return tmp;

    error:
        return NULL;
}

void swap_darrays(DArray *r1, DArray *r2) {
    DArray *tmp = r1;
    r1 = r2;
    r2 = tmp;
}

static inline void fill_data(metadata *m_data, uint64_t *mapped_file) {
    
    m_data->tuples = *mapped_file++;
    m_data->columns = *mapped_file++;
    m_data->data = MALLOC(relation *, m_data->columns);

    for (size_t j = 0 ; j < m_data->columns ; j++) {
        relation *rel = MALLOC(relation, 1);
        rel->tuples = DArray_create(sizeof(tuple), m_data->tuples);
        m_data->data[j] = rel;
        uint64_t max_value;
        uint64_t min_value;
        for (size_t i = 0 ; i < m_data->tuples ; i++) {
            uint64_t current_key = *mapped_file++;
            tuple tup;
            tup.key = current_key;
            tup.payload = i;
            DArray_push(rel->tuples, &tup);
            
            if (i == 0) {
                max_value = current_key;
                min_value = current_key;
            }
            else {
                if (current_key > max_value) {
                    max_value = current_key;
                }
                if (current_key < min_value) {
                    min_valuae = current_key;
                }
            } 
        }
        bool is_smaller = false;
        if (max_value - min_value + 1 <= N) {
            is_smaller = true;
        }
        size_t array_size = is_smaller ? max_value - min_value + 1 : N;
        bool *distinct = CALLOC(array_size, sizeof(bool), bool);
        for (size_t i = 0 ; i < DArray_count(rel->tuples) ; i++) {
            uint64_t key = ((tuple *) DArray_get(rel->tuples, i))->key;
            if (is_smaller) {
                distinct[key - min_value] = true;
            } 
            else {
                distinct[(key - min_value) % array_size] = true;
            }
        }
        
        uint32_t distinct_values = 0;
        for (size_t i  = 0 ; i < DArray_count(rel->tuples) ; i++) {
            distinct_values++;
        
        }
        rel->stats = MALLOC(statistics, 1);
        rel->stats->max_value = max_value;
        rel->stats->min_value = min_value;
        rel->stats->distinct_values = distinct_values;
        rel->stats->array = distinct;
        rel->stats->array_size = array_size;
        rel->stats->approx_elements = DArray_count(rel->tuples);
    }
}

static void update_non_eq(uint32_t k1, uint32_t k2, relation *rel) {
    
    if (k1 < rel->stats->min_value) {
        k1 = rel->stats->min_value;
    }
    if (k2 > rel->stats->max_value) {
        k2 = rel->stats->max_value;
    }

    if (rel->stats->max_value - rel->stats->min_value > 0) {
        rel->stats->distinct_values *= (k2 - k1) / (rel->stats->max_value - rel->stats->min_value);
        rel->stats->approx_elements *= (k2 - k1) / (rel->stats->max_value - rel->stats->min_value);
    }
    rel->stats->min_value = k1;
    rel->stats->max_value = k2;
}

static long my_pow(size_t base, size_t power) {

    long result = 1;
    for (size_t i = 0 ; i < power ; i++) {
        result *= base;
    }
    return result;
}

static void update_other_columns(ssize_t column, ssize_t approx_elements, metadata *md, relation *rel) {
    for (ssize_t i = 0 ; i < md->columns ; i++) {
        if (i != column) {
            relation *current_rel = md->data[i];
            size_t approx_tmp = rel->stats->approx_elements;
            size_t distinct_tmp = rel->stats->distinct_values;
            if (approx_elements != 0 && distinct_tmp != 0) {
                long val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_tmp / distinct_tmp);
                current_rel->stats->distinct_values *= (1 - val);
            }
            current_rel->stats->approx_elements = rel->stats->approx_elements;
        }
    }
}

void update_statistics(query *qry, DArray *metadata_arr) {

    for (ssize_t i = 0 ; i < qry->predicates_size ; i++) {
        predicate current = qry->predicates[i].type;
        if (current.type == 1) {
            /*If its a filter*/
            
            uint32_t row = qry->relations[current.first.relation];
            uint32_t column = current.first.column;
            metadata *md = (metadata *) DArray_get(metadata_arr, row);
            relation *rel = md->data[column];

            bool found = false;
            
            size_t approx_elements = rel->stats->approx_elements;

            if (current.operator == EQ) {
                uint32_t number = *(uint32_t *) current.second;
                            
                if ( (number - rel->stats->min_value) <= rel->stats->array_size && rel->stats->array[number - rel->stats->min_value]) {
                    rel->stats->approx_elements = approx_elements / rel->stats->distinct_values;
                    rel->stats->distinct_values = 1;
                    rel->stats->array_size = 1;
                    FREE(rel->stats->array);
                    rel->stats->array = MALLOC(bool, 1);
                    rel->stats->array[0] = true;
                }
                else {
                    rel->stats->approx_elements = 0;
                    rel->stats->distinct_values = 0;
                }
                rel->stats->max_value = number;
                rel->stats->min_value = number;
            }
            else if (current.operator != SELF_JOIN) {
                /*Check if we have something like R.A > 5000 & R.A < 8000*/
                for (ssize_t j = i + 1 ; j < qry->predicates_size ; j++) {
                    predicate tmp = qry->predicates[j];
                    if (tmp.type == 0) {
                        break;
                    }
                    else if (current.first.relation == tmp.first.relation && current.first.column == tmp.first.column) {
                        uint32_t k1 = *(uint32_t *) current.second;
                        uint32_t k2 = *(uint32_t *) tmp.second;

                        if ((current.operator == LEQ && tmp.operator == GEQ) || (current.operator == L && tmp.operator == G)) {
                            /*If its tmp >= k2, current <= k1, make it tmp <= k2, current >= k1*/
                            uint32_t temp = k1;
                            k1 = k2;
                            k2 = temp;
                        }
                        
                        update_non_eq(k1, k2, rel);    
                        
                        found = true;
                        break;
                    }
                }
                if (!found && (current.operator == LEQ || current.operator == L)) {
                    /*We have only filter of type R.A < 5000, set k1 to be min*/
                    uint32_t k1 = rel->stats->min_value;
                    uint32_t k2 = *(uint32_t *) current.second;

                    update_non_eq(k1, k2, rel);
                }
                else if (!found && (current.operator == GEQ || current.operator == G)) {
                    uint32_t k1 = *(uint32_t *) current.second;
                    uint32_t k2 = rel->stats->max_value;

                    update_non_eq(k1, k2, rel);
                }
            }
            else if (current.operator == SELF_JOIN) {
                relation_column rel_col = *(relation_column *) current.second;
                
                relation *relB = md->data[rel_col.column];
                uint64_t max_valA = rel->stats->max_value;
                uint64_t max_valB = relB->stats->max_value;
                uint64_t min_valA = rel->stats->min_value;
                uint64_t min_valB = relB->stats->min_value;
                uint64_t distinct_values = rel->stats->distinct_values;
                
                uint64_t max_val = max_valA > max_valB ? max_valA : max_valB;
                uint64_t min_val = min_valA < min_valB ? min_valA : min_valB;
                size_t n = max_val - min_val + 1;

                rel->stats->approx_elements = relB->stats->approx_elements = approx_elements / n;
                long val = 0;
                if (approx_elements != 0 && distinct_values != 0) {
                    val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_elements / distinct_values);
                }   
                rel->stats->distinct_values = distinct_values * (1 - val);  
                relB->stats->distinct_values = distinct_values * (1 - val);

                for (ssize_t j = 0 ; j < md->columns ; j++) {
                    if (j != column && j != rel_col.column) {
                        uint64_t approx_tmp = md->data[j]->stats->approx_elements;
                        uint64_t distinct_tmp = md->data[j]->stats->distinct_values;
                        if (distinct_tmp != 0 && approx_elements != 0) {
                            val = my_pow(1 - rel->stats->approx_elements / approx_elements, approx_tmp / distinct_tmp);
                            md->data[j]->stats->distinct_values *= (1 - val);
                        }
                        md->data[j]->stats->approx_elements = rel->stats->approx_elements;
                    }
                }
            }
            update_other_columns(column, approx_elements, md, rel);
       }
       else if (current.type == 0) {
            uint32_t first_relation = qry->relations[current.first.relation];
            uint32_t first_column = current.first.column;
            uint32_t second_relation = qry->relations[((relation_column *) current.second)->relation];
            uint32_t second_column = ((relation_column *) current.second)->column;
            
            metadata *md_A = (metadata *) DArray_get(metadata_arr, first_relation);
            metadata *md_B = (metadata *) DArray_get(metadata_arr, second_relation);
            relation *relA = md_A->data[first_column];
            relation *relB = md_B->data[second_column];

            relation *max_min_rel = relA->stats->min_value > relB->stats->min_value ? relA : relB;
            relation *min_max_rel = relA->stats->max_value < relB->stats->max_value ? relA : relB;

            uint32_t k1, k2;
            k1 = max_min_rel->stats->min_value;
            k2 = min_max_rel->stats->max_value;

            relA->stats->min_value = relB->stats->min_value = k1;
            relA->stats->max_value = relB->stats->max_value = k2;

            size_t tmp_distinctA = relA->stats->distinct_values;
            size_t tmp_distinctB = relB->stats->distinct_values;
            size_t tmp_approxA = relA->stats->approx_elements;
            size_t tmp_approxB = relB->stats->approx_elements;

            size_t n = k2 - k1 + 1;
            relA->stats->approx_elements = relB->stats->approx_elements = (tmp_approxA * tmp_approxB) / n;
            relA->stats->distinct_values = relB->stats->distinct_values = (tmp_distinctA * tmp_distinctB) / n;

            for (ssize_t tmp_i = 0 ; tmp_i < md_A->columns; tmp_i++) {
                if (tmp_i != first_column) {
                    relation *current_rel = md_A->data[tmp_i];
                    long val = 0;
                    if (tmp_distinctA != 0 && current_rel->stats->distinct_values != 0) {
                        val = my_pow(1 - (relA->stats->distinct_values / tmp_distinctA), current_rel->stats->approx_elements / current_rel->stats->distinct_values);
                    }
                    current_rel->stats->distinct_values *= (1 - val);
                    current_rel->stats->approx_elements = relA->stats->approx_elements;
                }
            }
            for (ssize_t tmp_i = 0 ; tmp_i < md_B->columns; tmp_i++) {
                if (tmp_i != second_column) {
                    relation *current_rel = md_B->data[tmp_i];
                    long val = 0;
                    if (tmp_distinctB != 0 && current_rel->stats->distinct_values != 0) {
                        val = my_pow(1 (relB->stats->distinct_values / tmp_distinctB), current_rel->stats->approx_elements / current_rel->stats->distinct_values);
                    }
                    current_rel->stats->distinct_values *= (1 - val);
                    current_rel->stats->approx_elements = relB->stats->approx_elements;
                }
            }
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

        munmap(mapped_file, sb.st_size);
        close(fd);
    }

    FREE(linptr);
    return 0;

    error:
        FREE(linptr);
        return -1;
}

exists_info relation_exists(DArray *mid_results_array, uint64_t relation, uint64_t predicate_id) {

    exists_info exists;
    exists.index = -1;
    for (ssize_t i = DArray_count(mid_results_array) - 1 ; i >= 0 ; i--) {
        DArray *mid_results = * (DArray **) DArray_get(mid_results_array, i);
        for (ssize_t j = 0 ; j < DArray_count(mid_results) ; j++) {
            mid_result *current = (mid_result *) DArray_get(mid_results, j);
            if (current->relation == relation && current->predicate_id == predicate_id) {
                exists.index = j;
                exists.mid_result = i;
                return exists;
            }
        }
    }

    return exists;
}

ssize_t relation_exists_current(DArray *mid_results, uint64_t relation, uint64_t predicate_id) {

    ssize_t found = -1;
    for (ssize_t i = 0 ; i < DArray_count(mid_results) ; i++) {
        mid_result res = *(mid_result *) DArray_get(mid_results, i);
        if ((res.relation == relation) && (res.predicate_id == predicate_id) ) {
            found = i;
        }
    }

    return found;
}

