//------------------------------------------------------------------------------
//! @file ParseCommentTest.cc
//! @author Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
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

#include "console/ConsoleOptions.hh"
#include "gtest/gtest.h"

TEST(ParseComment, ExtractsOneNonemptyTokenValue)
{
  std::vector<std::string> arguments{"register", "/eos/file", "--comment",
                                     "hello & = \"quoted\"", "size=1"};
  std::string comment, error;
  ASSERT_TRUE(eos::console::ExtractConsoleComment(arguments, comment, error));
  EXPECT_EQ(comment, "hello & = \"quoted\"");
  EXPECT_EQ(arguments, (std::vector<std::string>{"register", "/eos/file", "size=1"}));
}

TEST(ParseComment, AllowsCommentBeforeCommand)
{
  std::vector<std::string> arguments{"--comment", "audit", "version"};
  std::string comment, error;
  ASSERT_TRUE(eos::console::ExtractConsoleComment(arguments, comment, error));
  EXPECT_EQ(comment, "audit");
  EXPECT_EQ(arguments, (std::vector<std::string>{"version"}));
}

TEST(ParseComment, LeavesSimilarFlagsAndLiteralPathTextIntact)
{
  std::vector<std::string> arguments{"register", "/eos/a --comment b",
                                     "attr=user.note= --comment x", "--comments"};
  const auto original = arguments;
  std::string comment, error;
  ASSERT_TRUE(eos::console::ExtractConsoleComment(arguments, comment, error));
  EXPECT_TRUE(comment.empty());
  EXPECT_EQ(arguments, original);
}

TEST(ParseComment, StopsAtOptionTerminator)
{
  std::vector<std::string> arguments{"evict", "--", "--comment", "literal"};
  const auto original = arguments;
  std::string comment, error;
  ASSERT_TRUE(eos::console::ExtractConsoleComment(arguments, comment, error));
  EXPECT_TRUE(comment.empty());
  EXPECT_EQ(arguments, original);
}

TEST(ParseComment, RejectsMissingEmptyAndDuplicateValues)
{
  for (auto arguments : std::vector<std::vector<std::string>>{
           {"version", "--comment"},
           {"version", "--comment", ""},
           {"version", "--comment", "one", "--comment", "two"}}) {
    std::string comment, error;
    EXPECT_FALSE(eos::console::ExtractConsoleComment(arguments, comment, error));
    EXPECT_FALSE(error.empty());
  }
}
