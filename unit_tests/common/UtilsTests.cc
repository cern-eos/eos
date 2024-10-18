//------------------------------------------------------------------------------
// File: UtilsTests.hh
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2024 CERN/Switzerland                                  *
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

#include "unit_tests/common/Namespace.hh"
#include "common/Utils.hh"
#include "gtest/gtest.h"
#include <fstream>

EOSCOMMONTESTING_BEGIN

TEST(ParseUtils, SanitizeGeoTag)
{
  std::string geotag, sanitized;
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "a:b";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "a::b";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(geotag == sanitized);
  geotag = "a::b::c::d::e::f::";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "abcd::efgh::ijkl";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(geotag == sanitized);
  geotag = "abcd::ef::::gh::ijk";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "abcd::ef::spa ce::gh::ijk";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "abcd::ef_gh::ijk";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "abcd::ef::123456789::gh";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
  geotag = "::";
  sanitized = eos::common::SanitizeGeoTag(geotag);
  ASSERT_TRUE(sanitized != geotag);
}

TEST(ParseUtils, GetFileAdlerXs)
{
  // Create temporary file with some contents
  std::string fn_pattern = "/tmp/eos.unittest.XXXXXX";
  const std::string fn = MakeTemporaryFile(fn_pattern);
  ASSERT_TRUE(!fn.empty());
  std::ofstream file(fn);
  const std::string data = "Just some random input to compute adler checksum";
  file << data;
  file.close();
  std::string adler_xs;
  ASSERT_TRUE(eos::common::GetFileAdlerXs(adler_xs, fn));
  ASSERT_STREQ("b8601227", adler_xs.c_str());
  (void) unlink(fn.c_str());
}

TEST(ParseUtils, GetFileHexSha1)
{
  // Create temporary file with some contents
  std::string fn_pattern = "/tmp/eos.unittest.XXXXXX";
  const std::string fn = MakeTemporaryFile(fn_pattern);
  ASSERT_TRUE(!fn.empty());
  std::ofstream file(fn);
  const std::string data = "Just some random input to compute adler checksum";
  file << data;
  file.close();
  std::string hex_sha1;
  ASSERT_TRUE(eos::common::GetFileHexSha1(hex_sha1, fn));
  ASSERT_STREQ("5213647b3c1386dd91b768809aeb9dea7b2f9c28", hex_sha1.c_str());
  (void) unlink(fn.c_str());
}

TEST(ParseUtils, ComputeSize) {
  uint64_t size = 0;
  ComputeSize(size,0);
  ASSERT_EQ(0,size);
  ComputeSize(size,5);
  ASSERT_EQ(5,size);
  ComputeSize(size,-10);
  ASSERT_EQ(0,size);
}

EOSCOMMONTESTING_END
