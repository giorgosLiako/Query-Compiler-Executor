#ifndef IO_FUNCTIONS_H
#define IO_FUNCTIONS_H

#include <stdio.h>
#include <stdlib.h>
#include "alloc_free.h"
#include "dbg.h"
#include "structs.h"
#include "result_list.h"

int count_lines(char *, size_t *);

int parse_file(char *, size_t , relation *);

int write_to_file(result *);

#endif