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

    q->selects = selects;
    q->select_size = spaces + 1;
}

static void rename_relations(query *qry) {

    int32_t *removed = MALLOC(int32_t, qry->relations_size);
    
    for (size_t i = 0 ; i < qry->relations_size ; i++) {
        removed[i] = -1;
    }

    for (size_t i = 0 ; i < qry->relations_size ; i++) {
        for (size_t j = 0 ; j < qry->relations_size ; j++) {
            if (i != j && qry->relations[i] == qry->relations[j] && removed[i] == -1) {
                for (size_t z = 0 ; z < qry->predicates_size ; z++) {
                    if (qry->predicates[z].type == 1 && qry->predicates[z].first.relation == j) {
                        qry->predicates[z].first.relation = i;
                        removed[j] = j;
                    } 
                    else if (qry->predicates[z].type == 0) {
                        relation_column *second = (relation_column *) qry->predicates[z].second;
                        if (qry->predicates[z].first.relation == j) {
                            qry->predicates[z].first.relation = i;
                            removed[j] = j;
                        }
                        if (second->relation == j) {
                            second->relation = i;
                            removed[j] = j;
                        }
                    }
                }
                for (size_t z = 0 ; z < qry->select_size ; z++) {
                    if (qry->selects[z].relation == j) {
                        qry->selects[z].relation = i;
                    }
                }
            }
        }
    }

    size_t rel_size = qry->relations_size;
    for (size_t i = 0 ; i < rel_size ; i++) {
        if (removed[i] != -1) {
            for (size_t j = i ; j < rel_size - 1 ; j++) {
                qry->relations[j] = qry->relations[j + 1];
            }
            qry->relations_size--;
        }
    }


    FREE(removed);
}


DArray* parser() {
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
        
        parse_predicates(predicates, &new_query);
        
        parse_select(select,  &new_query);

        rename_relations(&new_query);
        
        DArray_push(queries, &new_query);

    }
    check(characters != -1, "Getline failed");

    FREE(line_ptr);
    return queries;

    error:
        DArray_destroy(queries);
        return NULL;
}