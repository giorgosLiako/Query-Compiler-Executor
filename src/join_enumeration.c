#include "structs.h"
#include "queries.h"
#include "DArray.h"
#include "join.h"
#include "best_tree.h"
#include <stdarg.h>
#include "set.h"


#define RightDeepOnly 1
#define LeftDeepOnly 0

int **graph;

//works for numbers smaller than 10
char *to_string(int rel) {
    char *s = (char *) malloc(sizeof(char)*2);
    sprintf(s, "%d", rel);
    return s;
}

char *copy_string(char* str) {
    char *s = (char *) malloc(strlen(str)*1);
    strcpy(s, str);
    return s;
}

int in_string(char* str, int rel) {
    char *rel_str = to_string(rel);
    int size = strlen(str);
    for (size_t i = 0; i < size; i++) {
        if (str[i] ==  rel_str[0]) return 1;
    }
    
    free(rel_str);
    return 0;
}

char *append_string(int rel, char* str) {
    char* new = (char *) malloc(sizeof(char)*(strlen(str) + 2));
    char* rel_str = to_string(rel);

    int str_index = 0, new_index = 0;
    for (size_t i = 0; i < strlen(str) + 1; i++) {
        if (str[str_index] > rel_str[0]){
            new[new_index++] = rel_str[0];
        } else {
            new[new_index++] = str[str_index++];
        }
    }
    new[str_index] = '\0';

    free(rel_str);
    return new;
    
}

tree *to_tree(int rel){
    tree *new;
    new = (tree*) malloc(sizeof(tree));
    new->left = NULL;
    new->right = NULL;
    new->type = 1;
    new->rel = rel;

    return new;
}

tree *create_join_tree(tree *t, int rel) {
    tree *best_tree = (tree*) malloc(sizeof(tree));
    best_tree->type = 0;
    best_tree->left = t;
    best_tree->right = to_tree(rel);
    
    
    return best_tree;
}

void predicate_graph(predicate *preds, int pred_size, int rel_size) {
    graph = (int**) malloc(rel_size*sizeof(int*));
    for (size_t i = 0; i < rel_size; i++) {
        graph[i] = (int*) malloc(rel_size*sizeof(int));

        for (size_t j = 0; j < rel_size; j++) graph[i][j] = 0;
    }

    for (size_t i = 0; i < pred_size; i++) {
        int index_1 = preds[i].first.relation;
        int index_2 = (*(relation_column *) preds[i].second).relation;

        graph[index_1][index_2]++;
        graph[index_2][index_1]++;
    }
    
}

int connected(char *str, int rel) {
    for (size_t i = 0; i < strlen(str); i++) 
        if (graph[str[i] - '0'][rel]) return 1;
    return 0;
}

void pseudo_filter(){

}

DArray *get_columns(int rel_a, int rel_b, predicate *predicates, int pred_size) {
    DArray *cols = DArray_create(sizeof(int), 2);
    for (size_t i = 0; i < pred_size; i++) {
        if (predicates[i].first.relation == rel_a && ((relation_column*)predicates[i].second)->relation == rel_b){
            DArray_push(cols, predicates[i].first.column);
            DArray_push(cols, ((relation_column*)predicates[i].second)->column);
        } else if (predicates[i].first.relation == rel_b && ((relation_column*)predicates[i].second)->relation == rel_a) {
            DArray_push(cols, ((relation_column*)predicates[i].second)->column);
            DArray_push(cols, predicates[i].first.column);
        }
    }
    return cols;
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

int calculate_statistics(metadata *md_a, metadata *md_b, int col_a, int col_b, int *min, int *max) {
    relation *a = md_a->data[col_a], *b = md_b->data[col_b];

    
    if (a->stats->min_value > b->stats->min_value) *min = a->stats->min_value;
    else *min = b->stats->min_value;

    if (a->stats->max_value > b->stats->max_value) *max = a->stats->max_value;
    else *max = b->stats->max_value;

    int f_a, f_b;

    f_a = ((*max - *min)/(a->stats->max_value - a->stats->min_value)) * (a->stats->approx_elements);
    f_b = ((*max - *min)/(b->stats->max_value - b->stats->min_value)) * (b->stats->approx_elements);

    int n = *max - *min + 1;
    return (f_a*f_b)/n;

}

int set_statistics(DArray *meta, int rel_a, int rel_b, predicate *preds, int pred_size, int *c_a, int *c_b, int *b_max, int *b_min){
    DArray *cols = get_columns(rel_a, rel_b, preds, pred_size);
    metadata *md_a = DArray_get(meta, rel_a), *md_b = DArray_get(meta, rel_b);
    int best = -1, best_c, best_min, best_max, min, max;
    for (size_t i = 0; i < DArray_count(cols);) {
        int col_a = *((int*) DArray_get(cols, i++)), col_b = *((int*) DArray_get(cols, i++));
        int cost = calculate_statistics(md_a, md_b, col_a, col_b, &min, &max);
        if (best == -1){
            best = i - 2;
            best_c = cost;
            best_max = max;
            best_min = min;
        } else if (best_c > cost) {
            best = i - 2;
            best_c = cost; 
            best_max = max;
            best_min = min;
            
        }
    }
    *b_max = best_max;
    *b_min = best_min;
    *c_a = *((int*) DArray_get(cols, best));
    *c_b = *((int*) DArray_get(cols, best+1));
    return best_c;

}

int set_statistics_nleaf(){

}


void dfs(tree *t, int *relations, predicate *predicates, int pred_size, int *res, DArray *meta) {
    if (t->left->left != NULL) dfs(t->left, relations, predicates, pred_size, res, meta);
    //TODO: calculate the f value
    if (t->left->left == NULL) {
        int rel_a = t->left->rel;
        int rel_b = t->left->right;
        int col_a, col_b, min, max;
        int f = set_statistics(meta, rel_a, rel_b, predicates, pred_size, &col_a, &col_b, &max, &min);
        t->f = f;
        t->max = max;
        t->min = min;
        t->left->col = col_a;
        t->right->col = col_b;
    } else {
        set_statistics_nleaf();
    }
    //res += t->left->rel
}

int cost(tree *t, int *relations, predicate *predicates, int pred_size, DArray *meta) {
    int res = 0;
    dfs(t, relations, predicates, pred_size, &res, meta);
    return res;
}


tree *dp_linear(int *relations, int rel_size, predicate *predicates, int pred_size, DArray *meta){
    queue *set = new_queue();
    predicate_graph(predicates, pred_size, rel_size);

    for (size_t i = 0; i < rel_size; i++) {
        char* n_set = to_string(i);
        BestTree_set(n_set, to_tree(i));
        push(set, copy_string(n_set));
    }

    for (size_t i = 0; i < rel_size - 1; i++) {
        int set_size = q_size(set);
        char* old_set = pop(set);

        for (size_t j = 0; j < set_size; j++) {

            for (size_t k = i + 1; k < rel_size; k++) {
                
                if (!in_string(old_set, k) && connected(old_set, k)) {
                    tree *curr_tree = create_join_tree(BestTree(old_set), k);
                    char *new_set = append_string(k, old_set);

                    //TODO cost
                    if (BestTree(new_set) == NULL || cost(BestTree(new_set), relations, predicates, pred_size, meta) > cost(curr_tree, relations, predicates, pred_size, meta) ){
                        if (BestTree(new_set) == NULL) push(set, copy_string(new_set));
                        BestTree_set(new_set, curr_tree);
                    } 
                
                }
            }
            
        }
        
    }
    return BestTree(pop(set));
    
}
//&& connected(set, relations[k])