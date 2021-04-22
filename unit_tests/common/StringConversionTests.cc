//------------------------------------------------------------------------------
// File: StringConversionTests.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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

#include "gtest/gtest.h"
#include "Namespace.hh"
#include "common/StringConversion.hh"
#include <regex>

EOSCOMMONTESTING_BEGIN

using namespace eos::common;

TEST(StringConversion, Seal_Unseal_Operation)
{
  std::string input = "/eos/dev/pic_and_poc";
  ASSERT_TRUE(input == StringConversion::SealXrdOpaque(input));
  ASSERT_TRUE(input == StringConversion::UnsealXrdOpaque(input));
  input = "/eos/dev/pic&poc";
  std::string expected = "/eos/dev/pic#AND#poc";
  ASSERT_TRUE(expected == StringConversion::SealXrdOpaque(input));
  ASSERT_TRUE(input == StringConversion::UnsealXrdOpaque(expected));
  input = "/eos/dev/&pic&and&poc&&";
  expected = "/eos/dev/#AND#pic#AND#and#AND#poc#AND##AND#";
  ASSERT_STREQ(expected.c_str(), StringConversion::SealXrdOpaque(input).c_str());
  ASSERT_STREQ(input.c_str(),
               StringConversion::UnsealXrdOpaque(expected).c_str());
}

TEST(StringConversion, ChecksumTranslations)
{
  char buff[4];
  buff[0] = 0xc2;
  buff[1] = 0x3b;
  buff[2] = 0x91;
  buff[3] = 0x38;
  ASSERT_TRUE(StringConversion::BinData2HexString(buff, 4, 4) == "c23b9138");
  std::string in_buf;
  in_buf.resize(4);
  memcpy(&in_buf[0], &buff[0], 4);
  ASSERT_TRUE(StringConversion::BinData2HexString
              (in_buf.c_str(), in_buf.capacity(), 4) == "c23b9138");
  ASSERT_TRUE(StringConversion::BinData2HexString
              (in_buf.c_str(), in_buf.capacity(), 4, ' ') == "c2 3b 91 38");
  size_t out_sz;
  auto new_buff = StringConversion::Hex2BinDataChar("c23b9138", out_sz);
  ASSERT_EQ(out_sz, 4);

  for (size_t i = 0; i < out_sz; ++i) {
    ASSERT_EQ(buff[i], new_buff.get()[i]);
  }

  // Wrongly specified checksum should be converted only partially
  buff[0] = 0x2a;
  buff[1] = 0x38;
  buff[2] = 0x17;
  buff[3] = 0x4b;
  std::string wrong_xs {"2a38174be"}; // has 9 chars!
  new_buff = StringConversion::Hex2BinDataChar(wrong_xs, out_sz);
  ASSERT_EQ(out_sz, 4);

  for (size_t i = 0; i < out_sz; ++i) {
    ASSERT_EQ(buff[i], new_buff.get()[i]);
  }

  ASSERT_FALSE(StringConversion::BinData2HexString(&buff[0], 4, 4) == wrong_xs);
  ASSERT_TRUE(StringConversion::BinData2HexString(&buff[0], 4, 4) == "2a38174b");
}

TEST(StringConversion, timebased_uuidstring){
  std::string uuid;
  uuid = eos::common::StringConversion::timebased_uuidstring();
  std::regex regexUuid("[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}");
  ASSERT_TRUE(std::regex_match(uuid,regexUuid));
}

TEST(GetSizeFromString, BasicSanity)
{
  uint64_t out;
  ASSERT_TRUE(StringConversion::GetSizeFromString("5", out));
  ASSERT_EQ(out, 5);
  ASSERT_TRUE(StringConversion::GetSizeFromString("5M", out));
  ASSERT_EQ(out, 5000000);
  ASSERT_TRUE(StringConversion::GetSizeFromString("9k", out));
  ASSERT_EQ(out, 9000);
  // Bug, this should be false :(
  ASSERT_TRUE(StringConversion::GetSizeFromString("pickles", out));
  ASSERT_EQ(out, 0);
}

EOSCOMMONTESTING_END
