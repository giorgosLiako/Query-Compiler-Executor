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
    int new_size = strlen(str) + 1;
    char* new = (char *) malloc(sizeof(char)*(new_size + 1));
    char* rel_str = to_string(rel);

    int str_index = 0, new_index = 0;
    
    while (str_index < strlen(str) && str[str_index] < rel_str[0])
    {
        new[new_index++] = str[str_index++];
    }
    new[new_index++] = rel_str[0];
    while (str_index < strlen(str))
    {
        new[new_index++] = str[str_index++];    
    }
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

 void calculate_statistics(int rel_a, int rel_b, int col_a, int col_b, statistics **stats, statistics *stat) {
   if (stats[rel_a][col_a].min_value > stats[rel_b][col_b].max_value ||
        stats[rel_b][col_b].min_value > stats[rel_a][col_a].max_value){
            stat->approx_elements = 0;
            return;
        }

    if (stats[rel_a][col_a].min_value > stats[rel_b][col_b].min_value) stat->min_value = stats[rel_a][col_a].min_value;
    else stat->min_value = stats[rel_b][col_b].min_value;

    if (stats[rel_a][col_a].max_value < stats[rel_b][col_b].max_value) stat->max_value = stats[rel_a][col_a].max_value;
    else stat->max_value = stats[rel_b][col_b].max_value;

    int f_a, f_b;
    f_a = ((stat->max_value - stat->min_value) * stats[rel_a][col_a].approx_elements)/(stats[rel_a][col_a].max_value - stats[rel_a][col_a].min_value);
    f_b = ((stat->max_value - stat->min_value) * stats[rel_b][col_b].approx_elements)/(stats[rel_b][col_b].max_value - stats[rel_b][col_b].min_value);

    int n = stat->max_value - stat->min_value + 1;
    stat->approx_elements = (f_a*f_b)/n;
    //printf("--->%d\n", stat->approx_elements);
}

tree *to_tree(int rel,const DArray *meta){
    tree *new;
    new = (tree*) malloc(sizeof(tree));
    new->left = NULL;
    new->right = NULL;
    new->type = 1;
    new->rel = rel;

    new->pred_size = 0;
    new->preds = (predicate*) malloc(sizeof(predicate)*relation_size);

    new->rel_size = 1;
    new->rels = (int*) malloc(sizeof(int)*relation_size);
    for (size_t i = 0; i < relation_size; i++) {
        new->rels[i] = -1;
    }
    
    new->rels[rel] = rel;
    


    //printf("relation %d\n", rel);
    metadata current_stat = *((metadata*) DArray_get(meta, rel));

    new->stats = (statistics**) malloc(sizeof(statistics*)*relation_size);
    for (size_t i = 0; i < relation_size; i++){
        new->stats[i] = NULL;
    }
    


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
    //if (p1.operator != p2.operator) return 0;
    //if (p1.type != p2.type) return 0;
    return 1;

}

tree *create_join_tree(tree *t, int rel, const DArray *meta, int *flag) {
    //printf("new relation -> %d\n", rel);
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
        int skip = 1;
        for (size_t j = 0; j < DArray_count(cols); j++) {
            skip = 0;
            predicate *pred = (predicate*) DArray_get(cols, j);
            int col_a = pred->first.column;
            int col_b = ((relation_column*) pred->second)->column;
            //printf("%d.%d %d.%d\n", rel_a, col_a, rel_b, col_b);
            calculate_statistics(rel_a, rel_b, col_a, col_b, n_tree->stats, &stat);
            if (stat.approx_elements == 0) {
                *flag = 1;
                return;
            }
            if (t_best_col_a == -1){
                //printf("***1\n");
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            } else if (best_stat.approx_elements > stat.approx_elements) {
                //printf("***2\n");
                t_best_col_a = col_a;
                t_best_col_b = col_b;
                best_stat = stat;
                best_pred = pred;
            }
        }
        if (skip) continue;
        if (best_rel_a == -1) {
            //printf("***1 relation %d\n", rel_a);
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = best_stat;
            best_pred_all = best_pred;
        } else if (best_stat_all.approx_elements > best_stat.approx_elements) {
            //printf("***2 relation %d\n", rel_a);
            best_rel_a = rel_a;
            best_rel_b = rel_b;
            best_col_a = t_best_col_a;
            best_col_b = t_best_col_b;
            best_stat_all = best_stat;
            best_pred_all = best_pred;
        }   
    }
    //printf("COSST = %d\n", best_stat_all.approx_elements);
    n_tree->rels[rel] = rel;
    n_tree->left->rel = best_rel_a;
    n_tree->left->col = best_col_a;
    n_tree->right->col = best_col_b;
    //printf("%d %d %d %d \n", best_rel_a, best_col_a, best_rel_b, best_col_b);
    n_tree->stats[best_rel_a][best_col_a] = best_stat_all;
    n_tree->stats[best_rel_b][best_col_b] = best_stat_all;

    //investigate
    for (size_t i = 0; i < n_tree->rel_size; i++) {
        if (n_tree->stats[i] == NULL) continue;
        metadata *md_a = DArray_get(meta, i);
        for (size_t j = 0; j < md_a->columns; j++) {
            if ((i == best_rel_a && j == best_col_a) ||
                (i == best_rel_b && j == best_col_b)) {
                    continue;
            }
            n_tree->stats[i][j].approx_elements = best_stat_all.approx_elements;
        }

    }

    n_tree->preds = t->preds;
    n_tree->preds[t->pred_size] = *best_pred_all;
    
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


int cost(tree *t){
    //printf("cost ->%d \n", t->stats[0][0].approx_elements);
    return t->stats[t->left->rel][t->left->col].approx_elements;
}


predicate *dp_linear(int rel_size, predicate *predicates, int pred_size,const DArray *meta, int *null_join){
    q_node *hashtab[101];
    for (size_t i = 0; i < 101; i++)
    {
        hashtab[i] = NULL;
    }
    
    queue *set = new_queue();
    predicate_graph(predicates, pred_size, rel_size);
    //set_up_statistics(rel_size, meta);
    all_predicates = predicates;
    predicate_size = pred_size;
    relation_size = rel_size;
    int flag = 0;

    for (size_t i = 0; i < rel_size; i++) {
        char* n_set = to_string(i);
        BestTree_set(n_set, to_tree(i, meta), hashtab);
        push(set, copy_string(n_set));
    }

    for (size_t i = 0; i < rel_size - 1; i++) {
        //printf("----------------------\n");
        int set_size = q_size(set);
        //printf("set size %d\n", set_size);
        for (size_t j = 0; j < set_size; j++) {
            char* old_set = pop(set);

            for (size_t k = 0; k < rel_size; k++) {
                //printf("old set %s new relation %d\n", old_set, k);
                if (!in_string(old_set, k) && connected(old_set, k)) {
                    tree *curr_tree = create_join_tree(BestTree(old_set, hashtab), k, meta, &flag);
                    if (flag) {
                        *null_join = 1;
                        return;
                    }
                    char *new_set = append_string(k, old_set);
                    //printf("new set %s\n", new_set);
                    
                    //printf("%s %d\n", new_set, cost(curr_tree));
                    //printf("~\n");
                    if (BestTree(new_set, hashtab) == NULL || 
                        cost(BestTree(new_set, hashtab)) > cost(curr_tree) ){
                        
                        if (BestTree(new_set, hashtab) == NULL) push(set, copy_string(new_set));
                        BestTree_set(new_set, curr_tree, hashtab);
                    } 
                
                }
            }
            
        }
        
    }
    char* res = pop(set);
    //printf("result :%s cost :%d\n", res, cost(BestTree(res)));
   // print_predicates(BestTree(res)->preds, predicate_size);
    
    for (size_t i = 0; i < rel_size; i++) {
        free(graph[i]);
    }
    free(graph);
    
    //predicate *ret = BestTree(res, hashtab)->preds;

    return BestTree(res, hashtab)->preds;
    
}
//&& connected(set, relations[k])