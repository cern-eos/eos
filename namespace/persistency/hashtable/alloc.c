#include <stdlib.h>

#include "alloc.h"

// Dummy function for persistent memory allocation.
void persistent* pmalloc(size_t size)
{
    return malloc(size);
}

// Dummy function for persistent memory deallocation.
void pfree(void persistent* ptr)
{
    free(ptr);
}
