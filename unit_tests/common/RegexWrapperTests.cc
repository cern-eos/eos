//------------------------------------------------------------------------------
// File: RegexWrapperTests.cc
// Author: Elvin Sindrilaru - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "common/RegexWrapper.hh"
#include <gtest/gtest.h>
//#include <regex>

using namespace eos::common;

TEST(RegexWrapper, BasicTests)
{
  std::string sregex {"v[0-9]+(\\.[0-9]+)+"};
  ASSERT_TRUE(eos_regex_match("v5.3.11", sregex));
  ASSERT_FALSE(eos_regex_match("some.other.string", sregex));
  ASSERT_FALSE(eos_regex_match("Partial match of v5.3.10 is not good!", sregex));
  ASSERT_TRUE(eos_regex_search("Partial search of v5.3.10 is good!", sregex));
  ASSERT_FALSE(eos_regex_search("Partial search of random string is not good!",
                                sregex));
  // std::regex std_regex("[/\\w.]+");
  // ASSERT_TRUE(std::regex_search("path/to/file/test.exe", std_regex));
  // ASSERT_TRUE(std::regex_match("path/to/file/test.exe", std_regex));
  // ASSERT_TRUE(std::regex_match("/some_more_exec.", std_regex));
  // ASSERT_TRUE(std::regex_match("someword", std_regex));
  // ASSERT_TRUE(std::regex_match("/some_exec", std_regex));
  // ASSERT_FALSE(std::regex_match("not!a#good*word!", std_regex));
  // @note according to https://www.regular-expressions.info/gnu.html
  // shorthand character classes can not be used inside bracket expressions!
  // This means that the std::regex pattern above would fail below.
  sregex = "[/[:alnum:]_.]+";
  ASSERT_TRUE(eos_regex_search("path/to/file/test.exe", sregex));
  ASSERT_TRUE(eos_regex_match("path/to/file/test.exe", sregex));
  ASSERT_TRUE(eos_regex_match("/some_more_exec.", sregex));
  ASSERT_TRUE(eos_regex_match("someword", sregex));
  ASSERT_TRUE(eos_regex_match("/some_exec", sregex));
  ASSERT_FALSE(eos_regex_match("not!a#good*word!", sregex));
  sregex = "(lxplus)(.*)(.cern.ch)";
  ASSERT_TRUE(eos_regex_match("lxplus.cern.ch", sregex));
  ASSERT_TRUE(eos_regex_match("lxplus1234.cern.ch", sregex));
  ASSERT_FALSE(eos_regex_match("not_lxplus1234.cern.ch", sregex));
  ASSERT_FALSE(eos_regex_match("justmyhost.cern.ch", sregex));
  ASSERT_FALSE(eos_regex_match("lxplus1234.mytest.com", sregex));
  sregex = "(b)[789](.*)(.cern.ch)";
  ASSERT_TRUE(eos_regex_match("b9pgpun004.cern.ch", sregex));
  ASSERT_TRUE(eos_regex_match("b9p28p3894.cern.ch", sregex));
  ASSERT_FALSE(eos_regex_match("nonbatchhost.cern.ch", sregex));
  ASSERT_FALSE(eos_regex_match("b9p28p3ad.mytest.com", sregex));
}
