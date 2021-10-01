// Copyright 2008,2009,2010 Massachusetts Institute of Technology.
// All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

#ifndef LOGGING_CRC32C_H__
#define LOGGING_CRC32C_H__

#include <cstddef>
#include <stdint.h>

namespace checksum
{

/** Returns the initial value for a CRC32-C computation. */
static inline uint32_t crc32cInit()
{
  return 0xFFFFFFFF;
}

/** Pointer to a function that computes a CRC32C checksum.
@arg crc Previous CRC32C value, or crc32c_init().
@arg data Pointer to the data to be checksummed.
@arg length length of the data in bytes.
*/
typedef uint32_t (*CRC32CFunctionPtr)(uint32_t crc, const void* data,
                                      size_t length);

/** This will map automatically to the "best" CRC implementation. */
extern CRC32CFunctionPtr crc32c;

CRC32CFunctionPtr detectBestCRC32C();

/** Converts a partial CRC32-C computation to the final value. */
static inline uint32_t crc32cFinish(uint32_t crc)
{
  return ~crc;
}

uint32_t crc32cSarwate(uint32_t crc, const void* data, size_t length);
uint32_t crc32cSlicingBy4(uint32_t crc, const void* data, size_t length);
uint32_t crc32cSlicingBy8(uint32_t crc, const void* data, size_t length);
uint32_t crc32cHardware32(uint32_t crc, const void* data, size_t length);
uint32_t crc32cHardware64(uint32_t crc, const void* data, size_t length);

#ifdef __aarch64__
#define __builtin_ia32_crc32si __builtin_arm_crc32cw
#define __builtin_ia32_crc32hi __builtin_arm_crc32ch
#define __builtin_ia32_crc32qi __builtin_arm_crc32cb
// APPLE doesn't have a crc32di instruction equivalent, only arm_acle intrinsics, so
// this instruction needs a filter out, so we only use the 32bit HW accel for apple
#ifndef __APPLE__
#define __builtin_ia32_crc32di __builtin_aarch_crc32cx
#endif

#endif
}  // namespace checksum
#endif
