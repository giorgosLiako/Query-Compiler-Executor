#include "structs.h"
#include "queries.h"
#include "DArray.h"
#include "join.h"


#define RightDeepOnly 1
#define LeftDeepOnly 0

int compare_relation_column(relation_column r1, relation_column r2) {
    if (r1.column != r2.column) return 0;
    if (r1.relation != r2.relation) return 0;
    return 1;
}

typedef struct tree{
    void *right;
    void *left;
} tree;

typedef struct best_tree_node {
    DArray *key;
    tree *value;
    struct best_tree_node *next;
} best_tree_node;

typedef struct {
    best_tree_node *start;
    best_tree_node *end;
} best_tree_struct;

best_tree_struct *current_best_tree;

int sort_relation_column(relation_column *r_c1, relation_column *r_c2) {
    if (r_c1->relation > r_c2->relation) return 1;
    if (r_c1->column > r_c2->column) return 1;
    return 0;
}

//T2 can be a tree
tree *create_join_tree(tree *T1, relation_column *T2) {
    tree *best_tree = (tree*) malloc(sizeof(tree));
    if (!RightDeepOnly) {
        tree *test_tree;
        test_tree->left = T1;
        test_tree->right = T2;
        if (best_tree == NULL || cost(best_tree) > cost(test_tree)){
            best_tree->left = test_tree->left;
            best_tree->right = test_tree->right;
        }
    }
    if (!LeftDeepOnly) {
        tree *test_tree;
        test_tree->left = T2;
        test_tree->right = T1;
        if (best_tree == NULL || cost(best_tree) > cost(test_tree)){
            best_tree->left = test_tree->left;
            best_tree->right = test_tree->right;
        }
    }
    return best_tree;
}

DArray *to_set(relation_column r_l) {
    DArray *new_set = DArray_create(sizeof(relation_column*), 1);
    DArray_push(new_set, &r_l);
    return new_set;
}

void init_best_tree(){
    current_best_tree = (best_tree_struct*) malloc(sizeof(best_tree_struct));
}







tree* BestTree(void*);
tree* to_tree(relation_column);



DArray *create_set(relation_column *r_c, int r_c_size, int size){
    DArray *new_set = DArray_create(sizeof(DArray*), 1);
    for (size_t i = 0; i < r_c_size - 1; i++) {
        DArray *new_sub_set = DArray_create(sizeof(relation_column*), 1);
        DArray_push(new_sub_set, &r_c[i]);
        for (size_t j = i + 1; j < size; j++) {
            DArray_push(new_sub_set, &r_c[j]);
        }
        DArray_push(new_set, &new_sub_set);
    }
    return new_set;
}

int in_set(DArray *set, relation_column r_c) {
    for (size_t i = 0; i < DArray_count(set); i++) {
        if (compare_relation_column((*(relation_column*) DArray_get(set, i) ), r_c)){
            return 1;
        }
    }
    return 0;
}

void set_BestTree(DArray * set, tree* tr){
    DArray_sort(set, sort_relation_column);
    //TODO
    
}
int cost(tree*);

tree *dp_linear(relation_column *relation_column, int size){
    for (size_t i = 0; i < size; i++) {
        setBestTree(to_set(relation_column[i]), to_tree(relation_column[i]));
    }

    for (size_t i = 0; i < size; i++) {
        DArray *set = create_set(relation_column, size, i);
        for (size_t j = 0; j < DArray_count(set); j++) {
            for (size_t k = 0; k < size; k++) {
                if (!in_set(set, relation_column[i])) {
                    tree *curr_tree = create_join_tree(BestTree(set), relation_column[i]);
                
                    DArray *new_set = set;
                    DArray_push(new_set, &relation_column[i]);
                    if (BestTree(new_set) == NULL || cost(BestTree(new_set))>cost(curr_tree) ){
                        set_BestTree(new_set, curr_tree);
                    } 
                
                }
            }
            
        }
        
    }
    return BestTree(create_set(relation_column, size, size));
    
}