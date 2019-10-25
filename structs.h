
/* Type definition for a tuple */
struct tuple {
	
	int64_t key;
	int64_t payload;

};

/* Type definition for a relation.  It consists of an array of tuples and a size of the relation. */
struct relation {  

	tuple  *tuples;  
	uint64_t num_tuples;

};

struct resultNode {

	int64_t * buffer;
	struct resultNode * nextNode;
};


struct result {

	struct resultNode * ResultPtr;
};