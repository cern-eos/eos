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

#include "gtest/gtest.h"
#include "console/ConsoleMain.hh"

//------------------------------------------------------------------------------
// Test valid comment syntax
//------------------------------------------------------------------------------
TEST(ParseComment, ValidSyntax)
{
  std::string comment;
  char* line;
  // Arguments as they are
  line = (char*) "eos version --comment \"Hello Comment\"";
  std::string cmd = parse_comment(line, comment);
  ASSERT_STRNE(line, 0);
  ASSERT_STREQ(comment.c_str(), "\"Hello Comment\"");
  // Arguments quote-encased
  line = (char*) "eos \"version\" \"--comment\" \"Hello Comment\"";
  cmd = parse_comment(line, comment);
  ASSERT_FALSE(cmd.empty());
  ASSERT_STREQ(comment.c_str(), "\"Hello Comment\"");
}

//------------------------------------------------------------------------------
// Test invalid comment syntax
//------------------------------------------------------------------------------
TEST(ParseComment, InvalidSyntax)
{
  std::string comment;
  char* line;
  // Missing comment text
  line = (char*) "eos version --comment";
  std::string cmd = parse_comment(line, comment);
  ASSERT_TRUE(cmd.empty());
  ASSERT_TRUE(comment.empty());
  // Empty comment text
  line = (char*) "eos version --comment \"\"";
  cmd = parse_comment(line, comment);
  ASSERT_TRUE(cmd.empty());
  ASSERT_TRUE(comment.empty());
  // Missing starting quote for comment text
  line = (char*) "eos version --comment Hello Comment\"";
  cmd = parse_comment(line, comment);
  ASSERT_TRUE(cmd.empty());
  ASSERT_TRUE(comment.empty());
  // Missing ending quote for comment text
  line = (char*) "eos version --comment \"Hello Comment";
  cmd = parse_comment(line, comment);
  ASSERT_TRUE(cmd.empty());
  ASSERT_TRUE(comment.empty());
}

//------------------------------------------------------------------------------
// Test comment extraction
//------------------------------------------------------------------------------
TEST(ParseComment, CommentExtraction)
{
  std::string comment;
  char* line = (char*) "eos --comment \"Hello Comment\" version";
  std::string cmd = parse_comment(line, comment);
  ASSERT_STREQ(cmd.c_str(), "eos  version");
  ASSERT_STREQ(comment.c_str(), "\"Hello Comment\"");
}

//------------------------------------------------------------------------------
// Test no comment present
//------------------------------------------------------------------------------
TEST(ParseComment, NoCommentPresent)
{
  std::string comment;
  char* line;
  // Comment flag missing completely
  line = (char*) "eos version";
  std::string cmd = parse_comment(line, comment);
  ASSERT_STREQ(cmd.c_str(), "eos version");
  ASSERT_TRUE(comment.empty());
  // Similar flag containing --comment text
  line = (char*) "eos config dump --comments";
  cmd = parse_comment(line, comment);
  ASSERT_STREQ(cmd.c_str(), "eos config dump --comments");
  ASSERT_TRUE(comment.empty());
}
