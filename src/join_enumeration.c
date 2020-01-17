#include "join_enumeration.h"

int **graph;
//statistics **stats;
predicate *all_predicates;
int predicate_size;
int relation_size;


//works for numbers smaller than 10
char *to_string(int rel) {
    char *s = (char *) malloc(sizeof(char)*2);
    sprintf(s, "%d", rel);
    return s;
}

char *copy_string(char* str) {
    char *s = (char *) malloc(strlen(str)+1);
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
    while (new_index < strlen(str) + 1 && str_index != strlen(str)) {
        if (str[str_index] > rel_str[0]){
            new[new_index++] = rel_str[0];
            break;
        } else {
            new[new_index++] = str[str_index++];
        }
    }
    if (str_index != strlen(str)) new[new_index++] = str[str_index];
    else if (new_index != strlen(str)+1) new[new_index++] = rel_str[0];
    new[new_index] = '\0';

    free(rel_str);
    return new;
    
}

void get_columns(int rel_a, int rel_b, predicate *predicates, int pred_size, DArray *cols) {

    for (size_t i = 0; i < pred_size; i++) {
        if (predicates[i].first.relation == rel_a && ((relation_column*)predicates[i].second)->relation == rel_b){
            DArray_push(cols, &predicates[i]);
        } else if (predicates[i].first.relation == rel_b && ((relation_column*)predicates[i].second)->relation == rel_a) {
            DArray_push(cols, &predicates[i]);
        }
    }
    
}


// void calculate_statistics_one(int rel, int col_a, int col_b, statistics *stat) {
    
//     if (stats[rel][col_a].min_value > stats[rel][col_b].min_value) stat->min_value = stats[rel][col_a].min_value;
//     else stat->min_value = stats[rel][col_b].min_value;

//     if (stats[rel][col_a].max_value > stats[rel][col_b].max_value) stat->max_value = stats[rel][col_a].max_value;
//     else stat->max_value = stats[rel][col_b].max_value;

//     int n = stat->max_value - stat->min_value + 1;
//     stat->approx_elements = (stats[rel][col_a].approx_elements)/n;

// }

//  predicate get_predicate(int rel_a, int rel_b, int col_a, int col_b){
//      for (size_t i = 0; i < predicate_size; i++) {
//          if (   all_predicates[i].first.relation == rel_a &&
//                 all_predicates[i].first.column == col_a &&
//                 ((relation_column*)all_predicates[i].second)->relation == rel_b &&
//                 ((relation_column*)all_predicates[i].second)->column == col_b) {
//                 return all_predicates[i];
//             }
//      }
     
//  }

 void calculate_statistics(int rel_a, int rel_b, int col_a, int col_b, statistics **stats, statistics *stat) {
    
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

tree *to_tree(int rel,const DArray *meta){
    tree *new;
    new = (tree*) malloc(sizeof(tree));
    new->left = NULL;
    new->right = NULL;
    new->type = 1;
    new->rel = rel;

    new->pred_size = 0;
    new->preds = NULL;

    new->rel_size = 1;
    new->rels = (int*) malloc(sizeof(int)*relation_size);
    for (size_t i = 0; i < relation_size; i++) {
        new->rels[i] = -1;
    }
    
    new->rels[rel] = rel;
    


    printf("relation %d\n", rel);
    metadata current_stat = *((metadata*) DArray_get(meta, rel));

    new->stats = (statistics**) malloc(sizeof(statistics*)*relation_size);
    new->stats[rel] = (statistics*) malloc(sizeof(statistics)*current_stat.columns);
    for (size_t i = 0; i < current_stat.columns; i++) {
        new->stats[rel][i] = *(current_stat.data[i]->stats);
    }
    

    return new;
}

int predicate_compare(predicate p1, predicate p2) {
    if (p1.first.column != p2.first.column) return 0;
    if (p1.first.relation != p2.first.relation) return 0;
    if (((relation_column*) p1.second)->column != ((relation_column*) p2.second)->column) return 0;
    if (((relation_column*) p1.second)->relation != ((relation_column*) p2.second)->relation) return 0;
    if (p1.operator != p2.operator) return 0;
    if (p1.type != p2.type) return 0;
    return 1;

}

tree *create_join_tree(tree *t, int rel,const DArray *meta) {
    printf("new relation -> %d\n", rel);
    tree *n_tree = (tree*) malloc(sizeof(tree));
    n_tree->type = 0;
    n_tree->left = t;
    n_tree->right = to_tree(rel, meta);

    n_tree->rel_size = n_tree->left->rel_size + n_tree->right->rel_size;
    n_tree->rels = (int*) malloc(sizeof(int) * relation_size);
    for (size_t i = 0; i < relation_size; i++) {
        n_tree->rels[i] = t->rels[i];
    }
    

    n_tree->pred_size = 0;
    for (size_t i = 0; i < relation_size; i++) {
        if (t->rels[i] != -1) n_tree->pred_size += graph[t->rels[i]][rel];
    } 
    n_tree->pred_size += t->pred_size;
    n_tree->preds = (predicate*) malloc(sizeof(predicate)*n_tree->pred_size);

    metadata current_stat = *((metadata*) DArray_get(meta, rel));


    n_tree->stats = t->stats;
    n_tree->stats[rel] = (statistics*) malloc(sizeof(statistics)*current_stat.columns);
    for (size_t i = 0; i < current_stat.columns; i++) {
        n_tree->stats[rel][i] = *(current_stat.data[i]->stats);
    }
    
    

    n_tree->preds = (predicate*) malloc(sizeof(predicate)*n_tree->pred_size);
    memcpy(n_tree->preds, t->preds, t->pred_size*sizeof(predicate));
    
    int best_rel_a = -1;
    int best_rel_b;
    int best_col_a, best_col_b;
    statistics best_stat_all;
    predicate *best_pred_all = NULL;
    
    for (size_t i = 0; i < relation_size; i++) {
        //printf("best rel a -> %d \n", best_rel_a);
        if (n_tree->rels[i] == -1) continue;
        int rel_a = n_tree->rels[i];
        int rel_b = rel;

        DArray *cols = DArray_create(sizeof(predicate), 1);
        get_columns(rel_a, rel_b, all_predicates, predicate_size, cols);
        if (DArray_count(cols) == 0) continue;
        //printf("%d %d %d\n", rel_a, rel_b, DArray_count(cols));
        int t_best_col_a = -1;
        int t_best_col_b;
        statistics stat, best_stat;
        predicate *best_pred;
        for (size_t j = 0; j < DArray_count(cols); j++) {
            predicate *pred = (predicate*) DArray_get(cols, j);
            int col_a = pred->first.column;
            int col_b = ((relation_column*) pred->second)->column;
            //printf("%d.%d %d.%d\n", rel_a, col_a, rel_b, col_b);
            calculate_statistics(rel_a, rel_b, col_a, col_b, n_tree->stats, &stat);
            
            if (t_best_col_a == -1){
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            } else if (best_stat.approx_elements > stat.approx_elements) {
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            }

        }
        if (best_rel_a == -1) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = stat;
            best_pred_all = best_pred;
        } else if (best_stat_all.approx_elements > best_stat.approx_elements) {
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = stat;
            best_pred_all = best_pred;
        }   
    }
    //printf("SIZE = %d\n", n_tree->pred_size);
    n_tree->rels[rel] = rel;
    n_tree->left->rel = best_rel_a;
    n_tree->left->col = best_col_a;
    n_tree->right->col = best_col_b;
    //printf("%d %d %d %d \n", best_rel_a, best_col_a, best_rel_b, best_col_b);
    n_tree->stats[best_rel_a][best_col_a] = best_stat_all;
    n_tree->stats[best_rel_b][best_col_b] = best_stat_all;

    //investigate
    for (size_t i = 0; i < n_tree->rel_size; i++) {
        if (i != best_rel_a || i != best_rel_b) continue;
        metadata *md_a = DArray_get(meta, i);
        for (size_t j = 0; j < md_a->columns; j++) {
            if ((i == best_rel_a && j != best_col_a) ||
                (i == best_rel_b && j != best_col_b)) {
                    n_tree->stats[i][j].approx_elements = best_stat_all.approx_elements;
                }
        }

    }

    for (size_t i = 0; i < t->pred_size; i++) {
        n_tree->preds[i] = t->preds[i];
    }
    
    if (best_pred_all == NULL) return NULL;
    else n_tree->preds[t->pred_size] = *best_pred_all;
    
    size_t i = t->pred_size + 1;
    for (size_t j = 0; j < predicate_size && i < n_tree->pred_size; j++) {
        if(all_predicates[j].first.relation == rel || ((relation_column*)all_predicates[j].second)->relation == rel){
            if (predicate_compare(all_predicates[j], n_tree->preds[t->pred_size])) continue;
            else n_tree->preds[i++] = all_predicates[j];
        }
    }
        
    
    
    
    return n_tree;
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



// int set_statistics(DArray *meta, tree *t, predicate *preds, int pred_size){
//     int rel_a = t->left->rel;
//     int rel_b = t->right->rel;

//     DArray *cols = DArray_create(sizeof(int), 2);
//     get_columns(rel_a, rel_b, preds, pred_size, cols);
    
//     int best_col_a = -1;
//     int best_col_b;

//     statistics stat, best_stat;
//     for (size_t i = 0; i < DArray_count(cols);) {
//         int col_a = *((int*) DArray_get(cols, i++));
//         int col_b = *((int*) DArray_get(cols, i++));

//         calculate_statistics(rel_a, rel_b, col_a, col_b, &stat);

//         if (best_col_a == -1){
//             best_col_a = col_a;
//             best_col_b = col_b;
//             best_stat = stat;
//         } else if (best_stat.approx_elements > stat.approx_elements) {
//             best_col_a = col_a;
//             best_col_b = col_b;
//             best_stat = stat;
//         }
//     }

    
//     t->left->col = best_col_a;
//     t->right->col = best_col_b;
//     stats[rel_a][t->left->col] = best_stat;
//     stats[rel_b][t->right->col] = best_stat;

    
    
//     metadata *md_a = DArray_get(meta, rel_a);
//     metadata *md_b = DArray_get(meta, rel_b);

//     for (size_t i = 0; i < md_a->columns; i++) {
//         if (i != t->left->col) stats[rel_a][i].approx_elements = best_stat.approx_elements;
//     }
//     for (size_t i = 0; i < md_b->columns; i++) {
//         if (i != t->right->col) stats[rel_b][i].approx_elements = best_stat.approx_elements;
//     }
    
    
//     return best_stat.approx_elements;

// }

// int set_statistics_nleaf(DArray *relations, DArray *meta, tree *t, predicate *preds, int pred_size){
//     int best_rel_a = -1;
//     int best_rel_b;
//     int best_col_a, best_col_b;
//     statistics best_stat_all;
    
//     for (size_t i = 0; i < DArray_count(relations); i++) {
//         int rel_a = *((int*) DArray_get(relations, i));
//         int rel_b = t->right->rel;
        
//         DArray *cols = DArray_create(sizeof(int), 2);
//         get_columns(rel_a, rel_b, preds, pred_size, cols);
                
//         int t_best_col_a = -1;
//         int t_best_col_b;
//         statistics stat, best_stat;
//         for (size_t j = 0; j < DArray_count(cols);) {
//             int col_a = *((int*) DArray_get(cols, j++)), col_b = *((int*) DArray_get(cols, j++));
//             calculate_statistics(rel_a, rel_b, col_a, col_b, &stat);
            
//             if (t_best_col_a == -1){
//                 t_best_col_a = col_a;
//                 t_best_col_b = col_b;
//                 best_stat = stat;
//             } else if (best_stat.approx_elements > stat.approx_elements) {
//                 t_best_col_a = col_a;
//                 t_best_col_b = col_b;
//                 best_stat = stat;
//             }

//         }
//         if (best_rel_a == -1) {
//             best_rel_a = rel_a;
//             best_rel_b = rel_b;
//             best_col_a = t_best_col_a;
//             best_col_b = t_best_col_b;
//             best_stat_all = stat;
//         } else if (best_stat_all.approx_elements > best_stat.approx_elements) {
//             best_rel_a = rel_a;
//             best_rel_b = rel_b;
//             best_col_a = t_best_col_a;
//             best_col_b = t_best_col_b;
//             best_stat_all = stat;
//         }   
//     }

//     t->left->rel = best_rel_a;
//     t->left->col = best_col_a;
//     t->right->col = best_col_b;
//     stats[best_rel_a][best_col_a] = best_stat_all;
//     stats[best_rel_b][best_col_b] = best_stat_all;

//     for (size_t i = 0; i < DArray_count(relations); i++) {
//         metadata *md_a = DArray_get(meta, i);
//         for (size_t j = 0; j < md_a->columns; j++) {
//             if (i != t->left->rel && j != t->left->col) stats[i][j].approx_elements = best_stat_all.approx_elements;
//         }

//     }
    


//     metadata *md_b = DArray_get(meta, best_rel_b);
//     for (size_t i = 0; i < md_b->columns; i++) {
//         if (i != t->right->col) stats[best_rel_b][i].approx_elements = best_stat_all.approx_elements;
//     }
    
    
//     return best_stat_all.approx_elements;

    
// }

// void dfs(tree *t, DArray *relations, predicate *predicates, int pred_size, int *res, DArray *meta) {
//     if (t->left->left != NULL) dfs(t->left, relations, predicates, pred_size, res, meta);
    
//     if (t->left->left == NULL) {
//         res += set_statistics(meta, t->left, predicates, pred_size);
//         DArray_push(relations, &t->left->rel);
//         DArray_push(relations, &t->right->rel);
//     } else {
//         res += set_statistics_nleaf(relations, meta, t->left, predicates, pred_size);
//         DArray_push(relations, &t->right->rel);
//     }
// }

// int cost(tree *t, predicate *predicates, int pred_size, DArray *meta) {
//     int res = 0;
//     DArray *relations = DArray_create(sizeof(int), 2);
//     dfs(t, relations, predicates, pred_size, &res, meta);
//     return res;
// }

int cost(tree *t){
    return t->stats[t->left->rel][t->left->col].approx_elements;
}

// void set_up_statistics(int rel_size, DArray *meta){
//     stats = (statistics**) malloc(rel_size*sizeof(statistics*));
//     for (size_t i = 0; i < rel_size; i++) {
//         metadata *md = (metadata*) DArray_get(meta, i);
//         stats[i] = (statistics*) malloc(md->columns*sizeof(statistics));
//         for (size_t j = 0; j < md->columns; j++) {
//             stats[i][j] = *(md->data[j]->stats);
//         }
//     }
// }

predicate *dp_linear(int rel_size, predicate *predicates, int pred_size,const DArray *meta){
    queue *set = new_queue();
    predicate_graph(predicates, pred_size, rel_size);
    //set_up_statistics(rel_size, meta);
    all_predicates = predicates;
    predicate_size = pred_size;
    relation_size = rel_size;

    for (size_t i = 0; i < rel_size; i++) {
        char* n_set = to_string(i);
        BestTree_set(n_set, to_tree(i, meta));
        push(set, copy_string(n_set));
    }

    for (size_t i = 0; i < rel_size - 1; i++) {
        printf("----------------------\n");
        int set_size = q_size(set);
        printf("set size %d\n", set_size);
        for (size_t j = 0; j < set_size; j++) {
            char* old_set = pop(set);

            for (size_t k = j + 1; k < rel_size; k++) {
                printf("old set %s new relation %d\n", old_set, k);
                if (!in_string(old_set, k) && connected(old_set, k)) {
                    tree *curr_tree = create_join_tree(BestTree(old_set), k, meta);
                    char *new_set = append_string(k, old_set);
                    printf("new set %s\n", new_set);
                    //TODO cost
                    if (BestTree(new_set) == NULL || 
                        cost(BestTree(new_set)) > cost(curr_tree) ){
                        if (BestTree(new_set) == NULL) push(set, copy_string(new_set));
                        BestTree_set(new_set, curr_tree);
                    } 
                
                }
            }
            
        }
        
    }
    char* res = pop(set);
    
    print_predicates(BestTree(res)->preds, predicate_size);
    
    
    return BestTree(res)->preds;
    
}
//&& connected(set, relations[k])