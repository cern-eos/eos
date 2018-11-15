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

EOSCOMMONTESTING_BEGIN

using namespace eos::common;

TEST(StringConversion, Seal_Unseal_Opeation)
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

EOSCOMMONTESTING_END
