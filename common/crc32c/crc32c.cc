// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#include <cassert>
#include <cstdio>
#include <cstring>
#include <stdlib.h>
#include "crc32c.h"
#include "crc32ctables.h"

#undef __PIC__

namespace checksum
{

static uint32_t crc32c_CPUDetection(uint32_t crc, const void* data,
                                    size_t length)
{
  // Avoid issues that could potentially be caused by multiple threads: use a local variable
  CRC32CFunctionPtr best = detectBestCRC32C();
  crc32c = best;
  return best(crc, data, length);
}

CRC32CFunctionPtr crc32c = crc32c_CPUDetection;

static uint32_t cpuid(uint32_t functionInput)
{
  uint32_t ecx;
#if __SIZEOF_POINTER__ == 8
  uint32_t eax;
  uint32_t ebx;
  uint32_t edx;
#endif
#if __SIZEOF_POINTER__ == 8
#ifdef __PIC__
  // PIC: Need to save and restore ebx See:
  // http://sam.zoy.org/blog/2007-04-13-shlib-with-non-pic-code-have-inline-assembly-and-pic-mix-well
  asm("pushl %%ebx\n\t" /* save %ebx */
      "cpuid\n\t"
      "movl %%ebx, %[ebx]\n\t" /* save what cpuid just put in %ebx */
      "popl %%ebx" : "=a"(eax), [ebx] "=r"(ebx), "=c"(ecx), "=d"(edx) : "a"(functionInput)
      : "cc");
#else
#if !defined(__aarch64__)
  asm("cpuid" : "=a"(eax), "=b"(ebx), "=c"(ecx), "=d"(edx) : "a"(functionInput));
#endif // APPLE detection end
#endif
#else
  fprintf(stderr, "error: crc32c is not supported on 32bit platforms\n");
  ecx = 0;
#endif
  return ecx;
}

CRC32CFunctionPtr detectBestCRC32C()
{
  static const int SSE42_BIT = 20;
  uint32_t ecx = cpuid(1);
  bool hasSSE42 = ecx & (1 << SSE42_BIT);
  // test if living in a virtual machine
  int rc = system("dmidecode | egrep -i 'manufacturer|product' | grep 'Virtual Machine'");

  if (!WEXITSTATUS(rc)) {
    // fprintf(stderr,
    //"------ --:--:-- ----- CRC32C configured for virtual machines running without SSE42\n");
    hasSSE42 = 0;
  } else {
    if (hasSSE42) {
      //fprintf(stderr,
      //        "------ --:--:-- ----- CRC32C configured for machine with SSE42 extension\n");
    } else {
      //fprintf(stderr,
      //        "------ --:--:-- ----- CRC32C configured for machine without SSE42 extension\n");
    }
  }

  if (hasSSE42) {
#ifdef __LP64__
    return crc32cHardware64;
#else
    return crc32cHardware32;
#endif
  } else {
    return crc32cSlicingBy8;
  }
}

// Implementations adapted from Intel's Slicing By 8 Sourceforge Project
// http://sourceforge.net/projects/slicing-by-8/
/*++
 *
 * Copyright (c) 2004-2006 Intel Corporation - All Rights Reserved
 *
 * This software program is licensed subject to the BSD License,
 * available at http://www.opensource.org/licenses/bsd-license.html
 *
 * Abstract: The main routine
 *
 --*/
uint32_t crc32cSarwate(uint32_t crc, const void* data, size_t length)
{
  const char* p_buf = (const char*) data;
  const char* p_end = p_buf + length;

  while (p_buf < p_end) {
    crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
  }

  return crc;
}

uint32_t crc32cSlicingBy4(uint32_t crc, const void* data, size_t length)
{
  const char* p_buf = (const char*) data;
  // Handle leading misaligned bytes
  size_t initial_bytes = (sizeof(int32_t) - (intptr_t)p_buf) & (sizeof(
                           int32_t) - 1);

  if (length < initial_bytes) {
    initial_bytes = length;
  }

  for (size_t li = 0; li < initial_bytes; li++) {
    crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
  }

  length -= initial_bytes;
  size_t running_length = length & ~(sizeof(int32_t) - 1);
  size_t end_bytes = length - running_length;

  for (size_t li = 0; li < running_length / 4; li++) {
    crc ^= *(uint32_t*) p_buf;
    p_buf += 4;
    uint32_t term1 = crc_tableil8_o56[crc & 0x000000FF] ^
                     crc_tableil8_o48[(crc >> 8) & 0x000000FF];
    uint32_t term2 = crc >> 16;
    crc = term1 ^
          crc_tableil8_o40[term2 & 0x000000FF] ^
          crc_tableil8_o32[(term2 >> 8) & 0x000000FF];
  }

  for (size_t li = 0; li < end_bytes; li++) {
    crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
  }

  return crc;
}

uint32_t crc32cSlicingBy8(uint32_t crc, const void* data, size_t length)
{
  const char* p_buf = (const char*) data;
  // Handle leading misaligned bytes
  size_t initial_bytes = (sizeof(int32_t) - (intptr_t)p_buf) & (sizeof(
                           int32_t) - 1);

  if (length < initial_bytes) {
    initial_bytes = length;
  }

  for (size_t li = 0; li < initial_bytes; li++) {
    crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
  }

  length -= initial_bytes;
  size_t running_length = length & ~(sizeof(uint64_t) - 1);
  size_t end_bytes = length - running_length;

  for (size_t li = 0; li < running_length / 8; li++) {
    crc ^= *(uint32_t*) p_buf;
    p_buf += 4;
    uint32_t term1 = crc_tableil8_o88[crc & 0x000000FF] ^
                     crc_tableil8_o80[(crc >> 8) & 0x000000FF];
    uint32_t term2 = crc >> 16;
    crc = term1 ^
          crc_tableil8_o72[term2 & 0x000000FF] ^
          crc_tableil8_o64[(term2 >> 8) & 0x000000FF];
    term1 = crc_tableil8_o56[(*(uint32_t*)p_buf) & 0x000000FF] ^
            crc_tableil8_o48[((*(uint32_t*)p_buf) >> 8) & 0x000000FF];
    term2 = (*(uint32_t*)p_buf) >> 16;
    crc = crc ^ term1 ^
          crc_tableil8_o40[term2  & 0x000000FF] ^
          crc_tableil8_o32[(term2 >> 8) & 0x000000FF];
    p_buf += 4;
  }

  for (size_t li = 0; li < end_bytes; li++) {
    crc = crc_tableil8_o32[(crc ^ *p_buf++) & 0x000000FF] ^ (crc >> 8);
  }

  return crc;
}

// Hardware-accelerated CRC-32C (using CRC32 instruction)
uint32_t crc32cHardware32(uint32_t crc, const void* data, size_t length)
{
  const char* p_buf = (const char*) data;

  // alignment doesn't seem to help?
  for (size_t i = 0; i < length / sizeof(uint32_t); i++) {
    crc = __builtin_ia32_crc32si(crc, *(uint32_t*) p_buf);
    p_buf += sizeof(uint32_t);
  }

  // This ugly switch is slightly faster for short strings than the straightforward loop
  length &= sizeof(uint32_t) - 1;

  /*
    while (length > 0) {
    crc32bit = __builtin_ia32_crc32qi(crc32bit, *p_buf++);
    length--;
    }
  */
  switch (length) {
  case 3:
    crc = __builtin_ia32_crc32qi(crc, *p_buf++);

  case 2:
    crc = __builtin_ia32_crc32hi(crc, *(uint16_t*) p_buf);
    break;

  case 1:
    crc = __builtin_ia32_crc32qi(crc, *p_buf);
    break;

  case 0:
    break;

  default:
    // This should never happen; enable in debug code
    assert(false);
  }

  return crc;
}

// Hardware-accelerated CRC-32C (using CRC32 instruction)
uint32_t crc32cHardware64(uint32_t crc, const void* data, size_t length)
{
#if !defined(__LP64__) || (defined(__APPLE__) && defined(__aarch64__))
  return crc32cHardware32(crc, data, length);
#else
  const char* p_buf = (const char*) data;
  // alignment doesn't seem to help?
  uint64_t crc64bit = crc;

  for (size_t i = 0; i < length / sizeof(uint64_t); i++) {
    crc64bit = __builtin_ia32_crc32di(crc64bit, *(uint64_t*) p_buf);
    p_buf += sizeof(uint64_t);
  }

  // This ugly switch is slightly faster for short strings than the straightforward loop
  uint32_t crc32bit = (uint32_t) crc64bit;
  length &= sizeof(uint64_t) - 1;

  /*
    while (length > 0) {
    crc32bit = __builtin_ia32_crc32qi(crc32bit, *p_buf++);
    length--;
    }
  */
  switch (length) {
  case 7:
    crc32bit = __builtin_ia32_crc32qi(crc32bit, *p_buf++);

  case 6:
    crc32bit = __builtin_ia32_crc32hi(crc32bit, *(uint16_t*) p_buf);
    p_buf += 2;

  // case 5 is below: 4 + 1
  case 4:
    crc32bit = __builtin_ia32_crc32si(crc32bit, *(uint32_t*) p_buf);
    break;

  case 3:
    crc32bit = __builtin_ia32_crc32qi(crc32bit, *p_buf++);

  case 2:
    crc32bit = __builtin_ia32_crc32hi(crc32bit, *(uint16_t*) p_buf);
    break;

  case 5:
    crc32bit = __builtin_ia32_crc32si(crc32bit, *(uint32_t*) p_buf);
    p_buf += 4;

  case 1:
    crc32bit = __builtin_ia32_crc32qi(crc32bit, *p_buf);
    break;

  case 0:
    break;

  default:
    // This should never happen; enable in debug code
    assert(false);
  }

  return crc32bit;
#endif
}

}  // namespace checksum
