#include "structs.h"
#include "queries.h"
#include "DArray.h"
#include "join.h"
#include "best_tree.h"
#include <stdarg.h>
#include "set.h"

int **graph;
statistics **stats;

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


void get_columns(int rel_a, int rel_b, predicate *predicates, int pred_size, DArray *cols) {
    
    for (size_t i = 0; i < pred_size; i++) {
        if (predicates[i].first.relation == rel_a && ((relation_column*)predicates[i].second)->relation == rel_b){
            DArray_push(cols, &predicates[i].first.column);
            DArray_push(cols, &((relation_column*)predicates[i].second)->column);
        } else if (predicates[i].first.relation == rel_b && ((relation_column*)predicates[i].second)->relation == rel_a) {
            DArray_push(cols, &((relation_column*)predicates[i].second)->column);
            DArray_push(cols, &predicates[i].first.column);
        }
    }
}

void get_column_tree(tree *t, int rel, predicate *predicates, int pred_size, DArray *res) {
    if (t->left->type == 0){
        get_column_tree(t->left->left, rel, predicates, pred_size, res);
        get_columns(t->left->right->rel, rel, predicates, pred_size, res);
    } else {
        get_columns(t->left->rel, rel, predicates, pred_size, res);
    }
}

void calculate_statistics(int rel_a, int rel_b, int col_a, int col_b, statistics *stat) {
    
    if (stats[rel_a][col_a].min_value > stats[rel_b][col_b].min_value) stat->min_value = stats[rel_a][col_a].min_value;
    else stat->min_value = stats[rel_b][col_b].min_value;

    if (stats[rel_a][col_a].max_value > stats[rel_b][col_b].max_value) stat->max_value = stats[rel_a][col_a].max_value;
    else stat->max_value = stats[rel_b][col_b].max_value;

    int f_a, f_b;

    f_a = ((stat->max_value - stat->min_value) * stats[rel_a][col_a].approx_elements)/(stats[rel_a][col_a].max_value - stats[rel_a][col_a].min_value);
    f_b = ((stat->max_value - stat->min_value) * stats[rel_b][col_b].approx_elements)/(stats[rel_b][col_b].max_value - stats[rel_b][col_b].min_value);

    int n = stat->max_value - stat->min_value + 1;
    stat->approx_elements = (f_a*f_b)/n;

}

int set_statistics(DArray *meta, tree *t, predicate *preds, int pred_size){
    int rel_a = t->left->rel, rel_b = t->right->rel;
    DArray *cols = DArray_create(sizeof(int), 2);
    get_columns(rel_a, rel_b, preds, pred_size, cols);
    metadata *md_a = DArray_get(meta, rel_a), *md_b = DArray_get(meta, rel_b);
    int best = -1;
    statistics stat, best_stat;
    for (size_t i = 0; i < DArray_count(cols);) {
        int col_a = *((int*) DArray_get(cols, i++)), col_b = *((int*) DArray_get(cols, i++));
        calculate_statistics(rel_a, rel_b, col_a, col_b, &stat);
        if (best == -1){
            best = i - 2;
            best_stat = stat;
        } else if (best_stat.approx_elements > stat.approx_elements) {
            best = i - 2;
            best_stat = stat;
        }
    }

    
    t->left->col = *((int*) DArray_get(cols, best));
    t->right->col = *((int*) DArray_get(cols, best+1));
    stats[rel_a][t->left->col] = best_stat;
    stats[rel_b][t->right->col] = best_stat;

    

    for (size_t i = 0; i < md_a->columns; i++) {
        if (i != t->left->col) stats[rel_a][i].approx_elements = best_stat.approx_elements;
    }
    for (size_t i = 0; i < md_b->columns; i++) {
        if (i != t->right->col) stats[rel_b][i].approx_elements = best_stat.approx_elements;
    }
    
    
    return best_stat.approx_elements;

}

int set_statistics_nleaf(DArray *relations, DArray *meta, tree *t, predicate *preds, int pred_size){
    int best_rel_a = -1, best_rel_b = -1;
    int best_col_a, best_col_b;
    statistics best_stat_all;
    
    for (size_t i = 0; i < DArray_count(relations); i++) {
        int rel_a = *((int*) DArray_get(relations, i));
        int rel_b = t->right->rel;
        
        DArray *cols = DArray_create(sizeof(int), 2);
        get_columns(rel_a, rel_b, preds, pred_size, cols);
                
        int t_best_col_a = -1;
        int t_best_col_b;
        statistics stat, best_stat;
        for (size_t j = 0; j < DArray_count(cols);) {
            int col_a = *((int*) DArray_get(cols, j++)), col_b = *((int*) DArray_get(cols, j++));
            calculate_statistics(rel_a, rel_b, col_a, col_b, &stat);
            
            if (t_best_col_a == -1){
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
            } else if (best_stat.approx_elements > stat.approx_elements) {
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
            }

        }
        if (best_rel_a == -1) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = stat;
        } else if (best_stat_all.approx_elements > best_stat.approx_elements) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = stat;
        }   
    }

    t->left->rel = best_rel_a;
    t->left->col = best_col_a;
    t->right->col = best_col_b;
    stats[best_rel_a][best_col_a] = best_stat_all;
    stats[best_rel_b][best_col_b] = best_stat_all;

    for (size_t i = 0; i < DArray_count(relations); i++) {
        metadata *md_a = DArray_get(meta, i);
        for (size_t j = 0; j < md_a->columns; j++) {
            if (i != t->left->rel && j != t->left->col) stats[i][j].approx_elements = best_stat_all.approx_elements;
        }

    }
    


    metadata *md_b = DArray_get(meta, best_rel_b);
    for (size_t i = 0; i < md_b->columns; i++) {
        if (i != t->right->col) stats[best_rel_b][i].approx_elements = best_stat_all.approx_elements;
    }
    
    
    return best_stat_all.approx_elements;

    
}

void dfs(tree *t, DArray *relations, predicate *predicates, int pred_size, int *res, DArray *meta) {
    if (t->left->left != NULL) dfs(t->left, relations, predicates, pred_size, res, meta);
    
    if (t->left->left == NULL) {
        res += set_statistics(meta, t->left, predicates, pred_size);
        DArray_push(relations, &t->left->rel);
        DArray_push(relations, &t->right->rel);
    } else {
        res += set_statistics_nleaf(relations, meta, t->left, predicates, pred_size);
        DArray_push(relations, &t->right->rel);
    }
}

int cost(tree *t, predicate *predicates, int pred_size, DArray *meta) {
    int res = 0;
    DArray *relations = DArray_create(sizeof(int), 2);
    dfs(t, relations, predicates, pred_size, &res, meta);
    return res;
}

void set_up_statistics(int rel_size, DArray *meta){
    stats = (statistics**) malloc(rel_size*sizeof(statistics*));
    for (size_t i = 0; i < rel_size; i++) {
        metadata *md = (metadata*) DArray_get(meta, i);
        stats[i] = (statistics*) malloc(md->columns*sizeof(statistics));
        for (size_t j = 0; j < md->columns; j++) {
            stats[i][j] = *(md->data[j]->stats);
        }
    }
}

tree *dp_linear(int *relations, int rel_size, predicate *predicates, int pred_size, DArray *meta){
    queue *set = new_queue();
    predicate_graph(predicates, pred_size, rel_size);
    set_up_statistics(rel_size, meta);

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
                    if (BestTree(new_set) == NULL || cost(BestTree(new_set), predicates, pred_size, meta) > cost(curr_tree, predicates, pred_size, meta) ){
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