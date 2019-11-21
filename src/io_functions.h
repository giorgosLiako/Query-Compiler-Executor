#ifndef IO_FUNCTIONS_H
#define IO_FUNCTIONS_H

#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "dbg.h"
#include "structs.h"

int count_lines(char *, char *, size_t *);

int parse_file(char *, size_t , relation *);

#endif