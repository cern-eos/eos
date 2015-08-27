#ifndef HAVE_ALLOC_H
#define HAVE_ALLOC_H

// Stubbed persistency type modifiers
#define pstatic static
#define persistent

#ifdef __cplusplus
extern "C" {
#endif

void persistent * pmalloc(size_t size);
void pfree(void persistent * ptr);

#ifdef __cplusplus
}
#endif

#endif
