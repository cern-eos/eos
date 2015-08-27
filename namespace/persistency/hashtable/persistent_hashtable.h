#ifndef HAVE_PERSISTENT_HASTHABLE_H
#define HAVE_PERSISTENT_HASHTABLE_H

#include <stdlib.h>

#include "alloc.h"

#define BALANCE_DIRTY 0x80

typedef long long hash_value_t;

typedef struct persistent_hashtable_entry_t persistent_hashtable_entry_t;

struct persistent_hashtable_entry_t {
    persistent_hashtable_entry_t persistent * left;
    persistent_hashtable_entry_t persistent * right;
    char balance;
    const void persistent * key;
    const void persistent * value;
};

typedef hash_value_t (*hash_func_t)(const void*);
typedef int (*comp_func_t)(const void*, const void*);

typedef struct persistent_hashtable_t persistent_hashtable_t;

// Defines a hash table.
struct persistent_hashtable_t {
    hash_func_t hash_func;
    comp_func_t comp_func;
    size_t count;
    size_t bucket_count;

    // This array indicates the start of the actual buffer. This is not strict
    // ANSI C and relies on a GNU extension. However, this could easily be
    // replaced by an ISO C99 flexible array member or even an ISO C90 array
    // of length 1 (but in that case, the malloc call will need to be adjusted
    // as well).
    persistent_hashtable_entry_t persistent * buckets[0];
};

#ifdef __cplusplus
extern "C" {
#endif

persistent_hashtable_t persistent * hashtable_new(
    size_t bucket_count,
    hash_func_t hash_func,
    comp_func_t comp_func
);

persistent_hashtable_entry_t* hashtable_get(
    persistent_hashtable_t persistent * hash,
    const void* key
);

persistent_hashtable_entry_t persistent* hashtable_find(
    persistent_hashtable_t persistent * hash,
    const void* key
);

void hashtable_release(
    persistent_hashtable_t persistent * hash
);

void hashtable_remove(
    persistent_hashtable_t persistent * hash,
    const void persistent ** key,
    const void persistent ** value
);

void hashtable_clear(
    persistent_hashtable_t persistent * hash
);

#ifdef __cplusplus
}
#endif

#endif

// vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
