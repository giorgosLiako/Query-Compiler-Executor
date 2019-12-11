#define _GNU_SOURCE
#include "parsing.h"

static void parse_relations(char *string, query *q) {
    //find how many relations appear according to spaces
    size_t spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == ' ') {
            spaces++;
        }
    }
    
    uint32_t* relations = MALLOC(uint32_t, spaces + 1);

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

static void parse_predicates(char *string, query *q) {
   
    size_t ampersands = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == '&') {
            ampersands++;
        }
    } 

    predicate *predicates = MALLOC(predicate, ampersands + 1);
    
    char current_predicate[128];

    uint32_t v1,v2,v3,v4;
    char operator;
    char *ptr = string;
    int advance, i = 0;
    while (sscanf(ptr, "%127[^&]%n", current_predicate, &advance) == 1) {
        ptr += advance;

        if (sscanf(current_predicate, "%d.%d%c%d.%d", &v1, &v2, &operator, &v3, &v4) == 5){
            relation_column first;
            first.relation = v1;
            first.column = v2;

            relation_column *second = MALLOC(relation_column, 1);
            second->relation = v3;
            second->column = v4;

            predicates[i].type = 0;
            predicates[i].first = first;          
            predicates[i].second = second;
            predicates[i++].operator = operator;

        } else if (sscanf(current_predicate, "%u.%u%c%u", &v1, &v2, &operator, &v3) == 4) {
            relation_column first;   
            first.relation = v1;
            first.column = v2;

            uint32_t *second = MALLOC(uint32_t, 1);
            *second = v3;

            predicates[i].type = 1;
            predicates[i].first = first;            
            predicates[i].second = second;
            predicates[i++].operator = operator;
        }

        if (*ptr != '&') {
            break;
        }
        ptr++;
    }
    

    q->predicates = predicates;
    q->predicates_size = ampersands + 1;
    
}

static void parse_select(char* string, query *q) {
    //find how many relations appear according to spaces
    size_t spaces = 0;
    for (size_t i = 0; string[i] != '\0'; i++) {
        if (string[i] == ' ') {
            spaces++;
        }
    }

    relation_column* selects = MALLOC(relation_column, spaces + 1);

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

static inline int check_relations(query q, DArray *metadata_arr) {

    for (size_t i = 0 ; i < q.relations_size ; i++) {
        if (q.relations[i] > DArray_count(metadata_arr)) {
            log_err("Relation %u is invalid, exiting", q.relations[i]);
            return -1;
        }
    }
    return 0;
}

static inline int check_predicates(query q, DArray *metadata_arr) {

    for (size_t i = 0 ; i < q.predicates_size ; i++) {
        if (q.predicates[i].type == 1) {
            if (q.predicates[i].first.relation >= q.relations_size) {
                log_err("Predicate %lu is invalid! Relation id too big to match, exiting", i);
                return -1;
            }
            if (q.predicates[i].first.column >= ((metadata *) DArray_get(metadata_arr, q.predicates[i].first.relation))->columns) {
                log_err("Predicate %lu is invalid! Column with number %lu doesn't exist in this relation, exiting", i, q.predicates[i].first.column);
                return -1;
            }
        } else {            
            relation_column *rel_col = (relation_column *) q.predicates[i].second;
            if (q.predicates[i].first.relation >= q.relations_size || rel_col->relation >= q.relations_size) {
                log_err("Predicate %lu is invalid! Relation id too big to match, exiting", i);
                return -1;
            }
            if (q.predicates[i].first.column >= ((metadata *) DArray_get(metadata_arr, q.predicates[i].first.relation))->columns || rel_col->column >= ((metadata *) DArray_get(metadata_arr, rel_col->relation))->columns) {
                log_err("Predicate %lu is invalid! Couldn't match one of operands column to the relation, exiting", i);
                return -1;
            }
        }
    }
    return 0;
}

static int check_select(query q, DArray *metadata_arr) {

    for (size_t i = 0 ; i < q.select_size ; i++) {
        if (q.select[i].relation >= q.relations_size) {
            log_err("Select %lu is invalid! Relation id too big to match, exiting", i);
            return -1;
        }
        if (q.select[i].column >= ((metadata *) DArray_get(metadata_arr, q.select[i].relation))->columns) {
            log_err("Select %lu is invalid! Column too big to match at a current relation, exiting", i);
            return -1;
        }
    }
    return 0;
}

DArray* parser(DArray *metadata_arr) {
    DArray* queries = DArray_create(sizeof(query), 10);

    char* line_ptr = NULL;
    size_t n = 0;
    int characters;
    
    printf("%s\n", "Insert workloads");
    while ((characters = getline(&line_ptr, &n, stdin)) != -1) {
        
        if (line_ptr[0] == 'F') {
            break;
        }
        char relations[128], predicates[128], select[128];
       
        sscanf(line_ptr, "%[0-9 ]%*[|]%[0-9.=<>&]%*[|]%[0-9. ]", relations, predicates, select);

        query new_query;
        parse_relations(relations, &new_query);
      //  if (check_relations(new_query, metadata_arr) != 0) {
    //        goto error;
     //   }
        
        parse_predicates(predicates, &new_query);
    //    if (check_predicates(new_query, metadata_arr) != 0) {
     //       goto error;
     //   }
        
        parse_select(select,  &new_query);
       // if (check_select(new_query, metadata_arr) != 0) {
       //     goto error;
      //  }
        
        DArray_push(queries, &new_query);

    }
    check(characters != -1, "Getline failed");

    return queries;

    error:
        DArray_destroy(queries);
        return NULL;
}