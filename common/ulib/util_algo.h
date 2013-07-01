/* The MIT License

   Copyright (C) 2011, 2012 Zilong Tan (eric.zltan@gmail.com)

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

#ifndef _ULIB_UTIL_ALGO_H
#define _ULIB_UTIL_ALGO_H

#include <stddef.h>
#include <stdint.h>

#define _min(x, y) ({						\
			typeof(x) _min1 = (x);			\
			typeof(y) _min2 = (y);			\
			(void) (&_min1 == &_min2);		\
			_min1 < _min2 ? _min1 : _min2; })

#define _max(x, y) ({						\
			typeof(x) _max1 = (x);			\
			typeof(y) _max2 = (y);			\
			(void) (&_max1 == &_max2);		\
			_max1 > _max2 ? _max1 : _max2; })

#define min3(x, y, z) ({						\
			typeof(x) _min1 = (x);				\
			typeof(y) _min2 = (y);				\
			typeof(z) _min3 = (z);				\
			(void) (&_min1 == &_min2);			\
			(void) (&_min1 == &_min3);			\
			_min1 < _min2 ? (_min1 < _min3 ? _min1 : _min3) : \
				(_min2 < _min3 ? _min2 : _min3); })

#define max3(x, y, z) ({						\
			typeof(x) _max1 = (x);				\
			typeof(y) _max2 = (y);				\
			typeof(z) _max3 = (z);				\
			(void) (&_max1 == &_max2);			\
			(void) (&_max1 == &_max3);			\
			_max1 > _max2 ? (_max1 > _max3 ? _max1 : _max3) : \
				(_max2 > _max3 ? _max2 : _max3); })

/**
 * min_not_zero - return the minimum that is _not_ zero, unless both are zero
 * @x: value1
 * @y: value2
 */
#define min_not_zero(x, y) ({						\
			typeof(x) __x = (x);				\
			typeof(y) __y = (y);				\
			__x == 0 ? __y : ((__y == 0) ? __x : _min(__x, __y)); })

/**
 * clamp - return a value clamped to a given range with strict typechecking
 * @val: current value
 * @min: minimum allowable value
 * @max: maximum allowable value
 *
 * This macro does strict typechecking of min/max to make sure they are of the
 * same type as val.  See the unnecessary pointer comparisons.
 */
#define clamp(val, min, max) ({					\
			typeof(val) __val = (val);		\
			typeof(min) __min = (min);		\
			typeof(max) __max = (max);		\
			(void) (&__val == &__min);		\
			(void) (&__val == &__max);		\
			__val = __val < __min ? __min: __val;	\
			__val > __max ? __max: __val; })

#define min_t(type, x, y) ({					\
			type __min1 = (x);			\
			type __min2 = (y);			\
			__min1 < __min2 ? __min1: __min2; })

#define max_t(type, x, y) ({					\
			type __max1 = (x);			\
			type __max2 = (y);			\
			__max1 > __max2 ? __max1: __max2; })

/**
 * clamp_t - return a value clamped to a given range using a given type
 * @type: the type of variable to use
 * @val: current value
 * @min: minimum allowable value
 * @max: maximum allowable value
 *
 * This macro does no typechecking and uses temporary variables of type
 * 'type' to make all the comparisons.
 */
#define clamp_t(type, val, min, max) ({				\
			type __val = (val);			\
			type __min = (min);			\
			type __max = (max);			\
			__val = __val < __min ? __min: __val;	\
			__val > __max ? __max: __val; })

/**
 * clamp_val - return a value clamped to a given range using val's type
 * @val: current value
 * @min: minimum allowable value
 * @max: maximum allowable value
 *
 * This macro does no typechecking and uses temporary variables of whatever
 * type the input argument 'val' is.  This is useful when val is an unsigned
 * type and min and max are literals that will otherwise be assigned a signed
 * integer type.
 */
#define clamp_val(val, min, max) ({				\
			typeof(val) __val = (val);		\
			typeof(val) __min = (min);		\
			typeof(val) __max = (max);		\
			__val = __val < __min ? __min: __val;	\
			__val > __max ? __max: __val; })


#define _swap(a, b)							\
	do { typeof(a) __tmp = (a); (a) = (b); (b) = __tmp; } while (0)

/**
 * container_of - cast a member of a structure out to the containing structure
 * @ptr:	the pointer to the member.
 * @type:	the type of the container struct this is embedded in.
 * @member:	the name of the member within the struct.
 *
 */
#define container_of(ptr, type, member) ({				\
			const typeof( ((type *)0)->member ) *__mptr = (ptr); \
			(type *)( (char *)__mptr - __builtin_offsetof(type,member) );})

#define generic_compare(x, y) (((x) > (y)) - ((x) < (y)))

static inline void memswp(unsigned long *x, unsigned long *y, size_t size)
{
	unsigned long *p = x + size/sizeof(*x);;
	unsigned char *h, *v;

	while (x != p) {
		_swap(*x, *y);
		x++;
		y++;
	}

	h = (unsigned char *)x;
	v = (unsigned char *)y;

#if __WORDSIZE == 64
	switch (size & 7) {
	case 7: _swap(h[6], v[6]);
	case 6: _swap(h[5], v[5]);
	case 5: _swap(h[4], v[4]);
	case 4: _swap(h[3], v[3]);
#else
		switch (size & 3) {
#endif
		case 3: _swap(h[2], v[2]);
		case 2: _swap(h[1], v[1]);
		case 1: _swap(h[0], v[0]);
		}
	}

#endif	/* _ULIB_UTIL_ALGO_H */
