#include "structs.h"
#include "queries.h"
#include "DArray.h"
#include "join.h"
#include "best_tree.h"
#include <stdarg.h>
#include "set.h"


#define RightDeepOnly 1
#define LeftDeepOnly 0

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

int *predicate_graph(predicate *preds, int pred_size, int rel_size) {
    int **graph = (int**) malloc(rel_size*sizeof(int*));
    for (size_t i = 0; i < rel_size; i++) {
        graph[i] = (int*) malloc(rel_size*sizeof(int));

        for (size_t j = 0; j < rel_size; j++) graph[i][j] = 0;
    }

    for (size_t i = 0; i < pred_size; i++) {
        int index_1 = preds[i].first.relation;
        int index_2 = (*(relation_column *) preds[i].second).relation;

        graph[index_1][index_2] = 1;
        graph[index_2][index_1] = 1;
    }
    
    return graph;
}

int connected(int **graph, char *str, int rel) {
    for (size_t i = 0; i < strlen(str); i++) 
        if (graph[str[i] - '0'][rel]) return 1;
    return 0;
}


void dfs(tree *t, relation *relations, predicate *predicates, int *res) {
    if (t->left->type == 0) dfs(t->left, relations, predicates, res);
    //TODO: calculate the f value
    //res += t->left->rel
}

int cost(tree *t, relation *relations, predicate *predicates) {
    int res = 0;
    dfs(t, relations, predicates, &res);
    return res;
}


tree *dp_linear(relation *relations, int rel_size, predicate *predicates, int pred_size){
    queue *set = new_queue();
    int **g = predicate_graph(predicates, pred_size, rel_size);

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
                
                if (!in_string(old_set, k) && connected(g, old_set, k)) {
                    tree *curr_tree = create_join_tree(BestTree(old_set), k);
                    char *new_set = append_string(k, old_set);

                    //TODO cost
                    if (BestTree(new_set) == NULL || cost(BestTree(new_set), relations, predicates) > cost(curr_tree, relations, predicates) ){
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