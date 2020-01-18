#include "join.h"
#include "best_tree.h"



#define HASHSIZE 101


unsigned hash(char *s) {
    unsigned hashval;
    for (hashval = 0; *s != '\0'; s++)
      hashval = *s + 31 * hashval;
    return hashval % HASHSIZE;
}



q_node *find(char *s, q_node *hashtab[HASHSIZE]) {
    q_node *np;
    for (np = hashtab[hash(s)]; np != NULL; np = np->next)
        if (strcmp(s, np->name) == 0) return np;
    return NULL; 
}

tree *BestTree(char *s, q_node *hashtab[HASHSIZE]) {
    if (find(s, hashtab)) return find(s, hashtab)->tree;
    else return NULL;
}

tree *create(tree *t) {
    tree *p;
    p = (tree *) malloc(sizeof(tree));
    p = t;
    return p;
}


q_node *BestTree_set(char *name, tree *tree, q_node *hashtab[HASHSIZE]) {
    q_node *np;
    unsigned hashval;

    if ((np = find(name, hashtab)) == NULL) { 

        np = (q_node *) malloc(sizeof(q_node));
        if (np == NULL || (np->name = strdup(name)) == NULL) return NULL;
        
        hashval = hash(name);
        np->next = hashtab[hashval];
        hashtab[hashval] = np;

    } else {

        free((void *) np->tree); /*free previous tree */

    }

    if ((np->tree = create(tree)) == NULL) return NULL;

    return np;
}

void BestTree_delete(q_node *hashtab[HASHSIZE]){
    q_node *np;
    q_node *temp;
    for (size_t i = 0; i < HASHSIZE; i++) {
        for (np = hashtab[i]; np != NULL;){
            temp = np;
            np = np->next;
            free(temp);
        }
        hashtab[i] = NULL;
    }
    
}