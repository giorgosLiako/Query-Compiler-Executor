#include "join.h"
#include "best_tree.h"



#define HASHSIZE 101
static q_node *hashtab[HASHSIZE]; 


unsigned hash(char *s) {
    printf("%s \n", s);
    unsigned hashval;
    for (hashval = 0; *s != '\0'; s++)
      hashval = *s + 31 * hashval;
    return hashval % HASHSIZE;
}



q_node *find(char *s) {
    q_node *np;
    for (np = hashtab[hash(s)]; np != NULL; np = np->next)
        if (strcmp(s, np->name) == 0) return np;
    return NULL; 
}

tree *BestTree(char *s) {
    if (find(s)) return find(s)->tree;
    else return NULL;
}

tree *create(tree *t) {
    tree *p;
    p = (tree *) malloc(sizeof(tree));
    p = t;
    return p;
}


q_node *BestTree_set(char *name, tree *tree) {
    q_node *np;
    unsigned hashval;

    if ((np = find(name)) == NULL) { 

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