#ifndef JERASURE_VECTOROP_H
#define JERASURE_VECTOROP_H

#include <stdint.h>

// -------------------------------------------------------------------------
// constant used in the block alignment function to allow for vector ops
// -------------------------------------------------------------------------
#define LARGEST_VECTOR_WORDSIZE 16

// -------------------------------------------------------------------------
// switch to 128-bit XOR operations if possible
// -------------------------------------------------------------------------
#if __GNUC__ > 4 || \
  (__GNUC__ == 4 && (__GNUC_MINOR__ >= 4) ) || \
  (__clang__ == 1 )
#ifdef VECTOROP_DEBUG
#pragma message "* using 128-bit vector operations in " __FILE__
#endif

// -------------------------------------------------------------------------
// use 128-bit pointer
// -------------------------------------------------------------------------
typedef long vector_op_t __attribute__ ((vector_size (16)));
#define VECTOR_WORDSIZE 16
#else
// -------------------------------------------------------------------------
// use 64-bit pointer
// -------------------------------------------------------------------------
typedef unsigned long long vector_op_t;
#define VECTOR_WORDSIZE 8
#endif

#endif  /* JERAUSRE_VECTOROP_H */
