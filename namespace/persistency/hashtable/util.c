#include <inttypes.h>
#include <unistd.h>

#include "util.h"

inline int max(int a, int b)
{
    return a > b ? a : b;
}

inline void swap(const void** a, const void** b)
{
    const void* tmp;
    tmp = *a;
    *a = *b;
    *b = tmp;
}

inline void* align_next_page_boundary(void* start)
{
    uintptr_t pagesize = getpagesize();

    return (void*) (
        ((uintptr_t) start + (pagesize)) &
        (uintptr_t) ~(pagesize-1)
    );
}

// vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
