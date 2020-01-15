#include "join.h"

typedef struct tree {
    int type;
    int rel;
    int col;
    int f;
    int min;
    int max;
    struct tree *right;
    struct tree *left;
} tree;


typedef struct q_node { 
    struct q_node *next; 
    char *name; 
    tree *tree; 
} q_node;

#define HASHSIZE 101
static q_node *hashtab[HASHSIZE]; 


unsigned hash(char *s) {
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
    return find(s)->tree;
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
