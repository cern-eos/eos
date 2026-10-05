//------------------------------------------------------------------------------
//! @file Crc32cTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2026 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "common/crc32c/crc32c.h"
#include "gtest/gtest.h"
#include <array>
#include <vector>
#if defined(__x86_64__)
#include <cpuid.h>
#endif

TEST(Crc32c, Dispatch)
{
  auto expected = checksum::crc32cSlicingBy8;
#if defined(__x86_64__)
  uint32_t eax = 0;
  uint32_t ebx = 0;
  uint32_t ecx = 0;
  uint32_t edx = 0;

  if (__get_cpuid(1, &eax, &ebx, &ecx, &edx) && (ecx & bit_SSE4_2)) {
    expected = checksum::crc32cHardware64;
  }
#endif
  EXPECT_EQ(expected, checksum::detectBestCRC32C());
}

TEST(Crc32c, KnownChecksum)
{
  const char data[] = "123456789";
  EXPECT_EQ(0u,
            checksum::crc32cFinish(checksum::crc32c(checksum::crc32cInit(), data, 0)));
  EXPECT_EQ(0xe3069283u,
            checksum::crc32cFinish(checksum::crc32c(checksum::crc32cInit(), data, 9)));
  EXPECT_EQ(checksum::detectBestCRC32C(), checksum::crc32c);
}

TEST(Crc32c, ImplementationParity)
{
  std::vector<checksum::CRC32CFunctionPtr> implementations{checksum::crc32cSlicingBy4,
                                                           checksum::crc32cSlicingBy8,
                                                           checksum::detectBestCRC32C()};

  if (checksum::detectBestCRC32C() == checksum::crc32cHardware64) {
    implementations.push_back(checksum::crc32cHardware32);
  }

  std::array<uint8_t, 264> data{};

  for (size_t i = 0; i < data.size(); ++i) {
    data[i] = static_cast<uint8_t>(i * 37);
  }

  for (const uint32_t seed : {0u, 1u, 0x12345678u, 0xffffffffu}) {
    for (size_t offset = 0; offset < 8; ++offset) {
      for (size_t length = 0; length <= 256; ++length) {
        const auto expected = checksum::crc32cSarwate(seed, data.data() + offset, length);

        for (const auto implementation : implementations) {
          EXPECT_EQ(expected, implementation(seed, data.data() + offset, length))
              << "seed=" << seed << " offset=" << offset << " length=" << length;
          const auto first = implementation(seed, data.data() + offset, length / 2);
          EXPECT_EQ(expected, implementation(first, data.data() + offset + length / 2,
                                             length - length / 2))
              << "incremental seed=" << seed << " offset=" << offset
              << " length=" << length;
        }
      }
    }
  }
}
