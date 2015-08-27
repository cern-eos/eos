#ifndef HAVE_UTIL_H
#define HAVE_UTIL_H

#include <inttypes.h>

// Dummy macro to annotate blocks that are supposed to be atomic
#define ATOMIC

inline void swap(const void** a, const void** b);
inline int max(int a, int b);
inline void* align_next_page_boundary(void* start);

#endif

// vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
