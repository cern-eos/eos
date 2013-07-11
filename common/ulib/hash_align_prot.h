/* The MIT License

   Copyright (C) 2011, 2012, 2013 Zilong Tan (eric.zltan@gmail.com)
   Copyright (c) 2008, 2009, 2011 by Attractive Chaos <attractor@live.co.uk>

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   The above copyright notice and this permission notice shall be
   included in all copies or substantial portions of the Software.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
*/

#ifndef _ULIB_HASH_ALIGN_PROT_H
#define _ULIB_HASH_ALIGN_PROT_H

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "util_algo.h"

#if __WORDSIZE == 64

typedef uint64_t ah_iter_t;
typedef uint64_t ah_size_t;

#define AH_ISDEL(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 1      )
#define AH_ISEMPTY(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 2      )
#define AH_ISEITHER(flag, i)	 ( ((flag)[(i) >> 5] >> (((i) & 0x1fU) << 1)) & 3      )
#define AH_CLEAR_DEL(flag, i)	 (  (flag)[(i) >> 5] &= ~(1ul << (((i) & 0x1fU) << 1)) )
#define AH_CLEAR_EMPTY(flag, i)	 (  (flag)[(i) >> 5] &= ~(2ul << (((i) & 0x1fU) << 1)) )
#define AH_CLEAR_BOTH(flag, i)	 (  (flag)[(i) >> 5] &= ~(3ul << (((i) & 0x1fU) << 1)) )
#define AH_SET_DEL(flag, i)	 (  (flag)[(i) >> 5] |=	 (1ul << (((i) & 0x1fU) << 1)) )

#define AH_FLAGS_BYTE(nb)	 ( (nb) < 32? 8: (nb) >> 2 )

#else

typedef uint32_t ah_iter_t;
typedef uint32_t ah_size_t;

#define AH_ISDEL(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 1      )
#define AH_ISEMPTY(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 2      )
#define AH_ISEITHER(flag, i)	 ( ((flag)[(i) >> 4] >> (((i) & 0xfU) << 1)) & 3      )
#define AH_CLEAR_DEL(flag, i)	 (  (flag)[(i) >> 4] &= ~(1ul << (((i) & 0xfU) << 1)) )
#define AH_CLEAR_EMPTY(flag, i)	 (  (flag)[(i) >> 4] &= ~(2ul << (((i) & 0xfU) << 1)) )
#define AH_CLEAR_BOTH(flag, i)	 (  (flag)[(i) >> 4] &= ~(3ul << (((i) & 0xfU) << 1)) )
#define AH_SET_DEL(flag, i)	 (  (flag)[(i) >> 4] |=	 (1ul << (((i) & 0xfU) << 1)) )

#define AH_FLAGS_BYTE(nb)	 ( (nb) < 16? 4: (nb) >> 2 )

#endif

/* error codes for alignhash_set() */
enum {
	AH_INS_ERR = 0,	 /**< element exists */
	AH_INS_NEW = 1,	 /**< element was placed at a new bucket */
	AH_INS_DEL = 2	 /**< element was placed at a deleted bucket */
};

/* Double hashing can be specified by defining
 * AH_DOUBLE_HASHING. Double hashing is preferable only when either
 * the hash table is relatively small, i.e. the flag bit array fits
 * into the CPU cache, or the key hash value distribution is biased. */
#ifdef AH_DOUBLE_HASHING
#define AH_PROBING_STEP(h)	 ( ((h) ^ (h) >> 3) | 1 )
#define AH_LOAD_FACTOR		 0.85
#else
#define AH_PROBING_STEP(h)	 ( 1 )
#define AH_LOAD_FACTOR		 0.77
#endif

#define DEFINE_ALIGNHASH_RAW(name, key_t, keyref_t, val_t, ismap, hashfn, equalfn) \
	typedef struct {						\
		ah_size_t nbucket;					\
		ah_size_t nelem;					\
		ah_size_t noccupied;					\
		ah_size_t bound;					\
		ah_size_t *flags;					\
		key_t	  *keys;					\
		val_t	  *vals;					\
	} alignhash_##name##_t;						\
									\
	static inline alignhash_##name##_t *				\
	alignhash_init_##name()						\
	{								\
		return (alignhash_##name##_t*)				\
			calloc(1, sizeof(alignhash_##name##_t));	\
	}								\
									\
	static inline void						\
	alignhash_destroy_##name(alignhash_##name##_t *h)		\
	{								\
		if (h) {						\
			free(h->flags);					\
			free(h->keys);					\
			free(h->vals);					\
			free(h);					\
		}							\
	}								\
									\
	static inline void						\
	alignhash_clear_##name(alignhash_##name##_t *h)			\
	{								\
		if (h && h->flags) {					\
			memset(h->flags, 0xaa, AH_FLAGS_BYTE(h->nbucket)); \
			h->nelem  = 0;					\
			h->noccupied = 0;				\
		}							\
	}								\
									\
	static inline ah_iter_t						\
	alignhash_get_##name(const alignhash_##name##_t *h, keyref_t key) \
	{								\
		if (h->nbucket) {					\
			ah_size_t i, k, step, last;			\
			ah_size_t mask = h->nbucket - 1;		\
			k = hashfn(key);				\
			i = k & mask;					\
			step = AH_PROBING_STEP(k);			\
			last = i;					\
			while (!AH_ISEMPTY(h->flags, i) &&		\
			       (AH_ISDEL(h->flags, i) || !equalfn(h->keys[i], key))) { \
				i = (i + step) & mask;			\
				if (i == last)				\
					return h->nbucket;		\
			}						\
			return AH_ISEMPTY(h->flags, i)? h->nbucket : i;	\
		} else							\
			return 0;					\
	}								\
									\
	static inline int						\
	alignhash_resize_##name(alignhash_##name##_t *h, ah_size_t nbucket) \
	{								\
		ah_size_t *flags;					\
		key_t	  *keys;					\
		val_t	  *vals;					\
		ah_size_t  mask = nbucket - 1;				\
		ah_size_t  j, flen;					\
		if (h->nelem >= (ah_size_t)(nbucket * AH_LOAD_FACTOR))	\
			return -1;					\
		flen  = AH_FLAGS_BYTE(nbucket);				\
		flags = (ah_size_t *) malloc(flen);			\
		if (flags == NULL)					\
			return -1;					\
		memset(flags, 0xaa, flen);				\
		if (h->nbucket < nbucket) {				\
			keys = (key_t*)	realloc(h->keys, nbucket * sizeof(key_t)); \
			if (keys == NULL) {				\
				free(flags);				\
				return -1;				\
			}						\
			h->keys = keys;					\
			if (ismap) {					\
				vals = (val_t*)	realloc(h->vals, nbucket * sizeof(val_t)); \
				if (vals == NULL) {			\
					free(flags);			\
					return -1;			\
				}					\
				h->vals = vals;				\
			}						\
		}							\
		for (j = 0; j != h->nbucket; ++j) {			\
			if (AH_ISEITHER(h->flags, j) == 0) {		\
				key_t key = h->keys[j];			\
				val_t val;				\
				if (ismap) val = h->vals[j];		\
				AH_SET_DEL(h->flags, j);		\
				for (;;) {				\
					ah_size_t i, k, step;		\
					k = hashfn(key);		\
					i = k & mask;			\
					step = AH_PROBING_STEP(k);	\
					while (!AH_ISEMPTY(flags, i))	\
						i = (i + step) & mask;	\
					AH_CLEAR_EMPTY(flags, i);	\
					if (i < h->nbucket && AH_ISEITHER(h->flags, i) == 0) { \
						_swap(h->keys[i], key);	\
						if (ismap) _swap(h->vals[i], val); \
						AH_SET_DEL(h->flags, i); \
					} else {			\
						h->keys[i] = key;	\
						if (ismap) h->vals[i] = val; \
						break;			\
					}				\
				}					\
			}						\
		}							\
		if (h->nbucket > nbucket) {				\
			keys = (key_t*) realloc(h->keys, nbucket * sizeof(key_t)); \
			if (keys) h->keys = keys;			\
			if (ismap) {					\
				vals = (val_t*) realloc(h->vals, nbucket * sizeof(val_t)); \
				if (vals) h->vals = vals;		\
			}						\
		}							\
		free(h->flags);						\
		h->flags = flags;					\
		h->nbucket = nbucket;					\
		h->noccupied = h->nelem;				\
		h->bound = (ah_size_t)(h->nbucket * AH_LOAD_FACTOR);	\
		return 0;						\
	}								\
									\
	static inline ah_iter_t						\
	alignhash_set_##name(alignhash_##name##_t *h, keyref_t key, int *ret) \
	{								\
		ah_size_t i, x, k, step, mask, site, last;		\
		if (h->noccupied >= h->bound) {				\
			if (h->nbucket) {				\
				if (alignhash_resize_##name(h, h->nbucket << 1)) \
					return h->nbucket;		\
			} else {					\
				if (alignhash_resize_##name(h, 2))	\
					return h->nbucket;		\
			}						\
		}							\
		site = h->nbucket;					\
		mask = h->nbucket - 1;					\
		x = site;						\
		k = hashfn(key);					\
		i = k & mask;						\
		if (AH_ISEMPTY(h->flags, i))				\
			x = i;						\
		else {							\
			step = AH_PROBING_STEP(k);			\
			last = i;					\
			while (!AH_ISEMPTY(h->flags, i) &&		\
			       (AH_ISDEL(h->flags, i) || !equalfn(h->keys[i], key))) { \
				if (AH_ISDEL(h->flags, i))		\
					site = i;			\
				i = (i + step) & mask;			\
				if (i == last) {			\
					x = site;			\
					break;				\
				}					\
			}						\
			if (x == h->nbucket) {				\
				if (AH_ISEMPTY(h->flags, i) && site != h->nbucket) \
					x = site;			\
				else					\
					x = i;				\
			}						\
		}							\
		if (AH_ISEMPTY(h->flags, x)) {				\
			h->keys[x] = key;				\
			AH_CLEAR_BOTH(h->flags, x);			\
			++h->nelem;					\
			++h->noccupied;					\
			*ret = AH_INS_NEW;				\
		} else if (AH_ISDEL(h->flags, x)) {			\
			h->keys[x] = key;				\
			AH_CLEAR_BOTH(h->flags, x);			\
			++h->nelem;					\
			*ret = AH_INS_DEL;				\
		} else							\
			*ret = AH_INS_ERR;				\
		return x;						\
	}								\
									\
	static inline void						\
	alignhash_del_##name(alignhash_##name##_t *h, ah_iter_t x)	\
	{								\
		if (x != h->nbucket && !AH_ISEITHER(h->flags, x)) {	\
			AH_SET_DEL(h->flags, x);			\
			--h->nelem;					\
		}							\
	}

/* provide two versions, the C++ version uses reference to save the
 * parameter copying costs. */
#ifdef __cplusplus
#define DEFINE_ALIGNHASH(name, key_t, val_t, ismap, hashfn, equalfn)	\
	DEFINE_ALIGNHASH_RAW(name, key_t, const key_t &, val_t,		\
			     ismap, hashfn, equalfn)
#else
#define DEFINE_ALIGNHASH(name, key_t, val_t, ismap, hashfn, equalfn)	\
	DEFINE_ALIGNHASH_RAW(name, key_t, key_t, val_t, ismap,		\
			     hashfn, equalfn)
#endif

/*------------------------- Human Interface -------------------------*/

/* Identity hash function, converting a key to an integer. This coerce
 * the key to be of integer type or integer interpretable. */
#define alignhash_hashfn(key) (ah_size_t)(key)

/* boolean function that tests whether two keys are equal */
#define alignhash_equalfn(a, b) ((a) == (b))

/* alignhash type */
#define alignhash_t(name) alignhash_##name##_t

/* return the key/value associated with the iterator */
#define alignhash_key(h, x) ((h)->keys[x])
#define alignhash_value(h, x) ((h)->vals[x])

/* Core alignhash functions. */
#define alignhash_init(name) alignhash_init_##name()
#define alignhash_destroy(name, h) alignhash_destroy_##name(h)
#define alignhash_clear(name, h) alignhash_clear_##name(h)
/* The resize function is called automatically when certain load limit
 * is reached. Thus don't call it manually unless you have to. */
#define alignhash_resize(name, h, s) alignhash_resize_##name(h, s)

/* Insert a new element without replacement.
 * r will hold the error code as defined above.
 * Returns an iterator to the new or existing element. */
#define alignhash_set(name, h, k, r) alignhash_set_##name(h, k, r)
#define alignhash_get(name, h, k) alignhash_get_##name(h, k)
/* delete an element by iterator */
#define alignhash_del(name, h, x) alignhash_del_##name(h, x)
/* test whether an iterator is valid */
#define alignhash_exist(h, x) (!AH_ISEITHER((h)->flags, (x)))

/* Iterator functions. */
#define alignhash_begin(h) (ah_iter_t)(0)
#define alignhash_end(h) ((h)->nbucket)

/* number of elements in the hash table */
#define alignhash_size(h) ((h)->nelem)
/* return the current capacity of the hash table */
#define alignhash_nbucket(h) ((h)->nbucket)

#endif	/* _ULIB_HASH_ALIGN_PROT_H */
