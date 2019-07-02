//------------------------------------------------------------------------------
// File: StringTokenizerTests.cc
// Author: Mihai Patrascoiu <mihai.patrascoiu@cern.ch>
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2019 CERN/Switzerland                                  *
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
#include "XrdOuc/XrdOucString.hh"

#include "Namespace.hh"
#include "common/StringTokenizer.hh"


EOSCOMMONTESTING_BEGIN

using namespace eos::common;

/* The StringTokenizer class performs a 2-step tokenizing process.
 * Initially, lines are extracted from the input, with '\n' as the delimiter.
 * Afterwards, each line is tokenized into words, using ' ' as the delimiter.
 *
 * If the delimiters are found within quotes, tokenization will not happen
 * and they will be part of the same unit (line or token).
 */

TEST(StringTokenizer, EmptyInput)
{
  std::string empty;
  XrdOucString sempty;
  std::unique_ptr<StringTokenizer> tokenizer;

  tokenizer.reset(new StringTokenizer(0));
  ASSERT_EQ(tokenizer->GetLine(), nullptr);

  tokenizer.reset(new StringTokenizer(""));
  ASSERT_EQ(tokenizer->GetLine(), nullptr);

  tokenizer.reset(new StringTokenizer(empty));
  ASSERT_EQ(tokenizer->GetLine(), nullptr);

  tokenizer.reset(new StringTokenizer(sempty));
  ASSERT_EQ(tokenizer->GetLine(), nullptr);
}

TEST(StringTokenizer, GetLine)
{
  std::string input;
  std::unique_ptr<StringTokenizer> tokenizer;

  // Simple lines input
  input = "Hello Line 1\n"
          "Hello Line 2\n"
          "Hello Line 3";

  tokenizer.reset(new StringTokenizer(input));

  ASSERT_STREQ(tokenizer->GetLine(), "Hello Line 1");
  ASSERT_STREQ(tokenizer->GetLine(), "Hello Line 2");
  ASSERT_STREQ(tokenizer->GetLine(), "Hello Line 3");
  ASSERT_EQ(tokenizer->GetLine(), nullptr);

  // Lines containing '\n' delimiter within quotes
  input = "Hello Line 1 \"Quoted Line 1\nQuoted Line2\"\n"
          "Hello Line 2";

  tokenizer.reset(new StringTokenizer(input));
  ASSERT_STREQ(tokenizer->GetLine(),
               "Hello Line 1 \"Quoted Line 1\nQuoted Line2\"");
  ASSERT_STREQ(tokenizer->GetLine(), "Hello Line 2");
  ASSERT_EQ(tokenizer->GetLine(), nullptr);
}

TEST(StringTokenizer, GetToken)
{
  std::string input;
  std::unique_ptr<StringTokenizer> tokenizer;

  // Simple tokens
  input = "Input line";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(), "Input");
  ASSERT_STREQ(tokenizer->GetToken(), "line");
  ASSERT_EQ(tokenizer->GetToken(), nullptr);

  // Quoted tokens
  // -- Tokens should be returned without enclosing quotes
  input = "\"Quoted\" \"arguments\"";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(), "Quoted");
  ASSERT_STREQ(tokenizer->GetToken(), "arguments");
  ASSERT_EQ(tokenizer->GetToken(), nullptr);

  // Edge case quoted tokens
  // -- Escaped quotes should be left untouched
  input =
      "\\\"Double\\\" \"\\\"escaped\\\"\" \\\"\"quoted\"\\\" \"simple\" argument";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(), "\\\"Double\\\"");
  ASSERT_STREQ(tokenizer->GetToken(), "\\\"escaped\\\"");
  ASSERT_STREQ(tokenizer->GetToken(), "\\\"\"quoted\"\\\"");
  ASSERT_STREQ(tokenizer->GetToken(), "simple");
  ASSERT_STREQ(tokenizer->GetToken(), "argument");
  ASSERT_EQ(tokenizer->GetToken(), nullptr);

  // Tokens containing space delimiter and escaped quotes within quotes
  // -- Tokens should contain spaces and the escaped quotes
  input = "\"Token with \\\"quotes\\\" and spaces\" argument";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(), "Token with \\\"quotes\\\" and spaces");
  ASSERT_STREQ(tokenizer->GetToken(), "argument");
  ASSERT_EQ(tokenizer->GetToken(), nullptr);

  // Null line sanity check
  ASSERT_EQ(tokenizer->GetLine(), nullptr);
}

TEST(StringTokenizer, GetTokenUnquoted)
{
  std::string input;
  std::unique_ptr<StringTokenizer> tokenizer;

  // Simple tokens
  input = "Input line";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "Input");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "line");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(), nullptr);

  // Quoted tokens
  // -- Tokens should be returned without enclosing quotes
  input = "\"Quoted\" \"arguments\"";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "Quoted");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "arguments");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(), nullptr);

  // Edge case quoted tokens
  // -- Full quote unescaping should happen
  input =
      "\\\"Double\\\" \"\\\"escaped\\\"\" \\\"\"quoted\"\\\" \"simple\" argument";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "\\\"Double\\\"");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "\"escaped\"");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "\\\"\"quoted\"\\\"");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "simple");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "argument");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(), nullptr);


  // Tokens containing space delimiter and escaped quotes within quotes
  // -- Tokens should contain spaces and the unescaped quotes
  input = "\"Token with \\\"quotes\\\" and spaces\" argument";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "Token with \"quotes\" and spaces");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "argument");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(), nullptr);

  // Null line sanity check
  ASSERT_EQ(tokenizer->GetLine(), nullptr);
}

TEST(StringTokenizer, GetTokenEscapeAndFlag)
{
  std::string input;
  std::unique_ptr<StringTokenizer> tokenizer;

  // GetToken() with EscapeAnd flag
  input = "&Symbol& & \\& escaped";
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(), "#AND#Symbol#AND#");
  ASSERT_STREQ(tokenizer->GetToken(), "#AND#");
  ASSERT_STREQ(tokenizer->GetToken(), "\\&");
  ASSERT_STREQ(tokenizer->GetToken(), "escaped");
  ASSERT_EQ(tokenizer->GetToken(), nullptr);

  // GetTokenUnquoted() with EscapeAnd flag
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "#AND#Symbol#AND#");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "#AND#");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "\\&");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(), "escaped");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(), nullptr);

  // Get Token() without EscapeAnd flag
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetToken(false), "&Symbol&");
  ASSERT_STREQ(tokenizer->GetToken(false), "&");
  ASSERT_STREQ(tokenizer->GetToken(false), "\\&");
  ASSERT_STREQ(tokenizer->GetToken(false), "escaped");
  ASSERT_EQ(tokenizer->GetToken(false), nullptr);

  // Get TokenUnquoted() without EscapeAnd flag
  tokenizer.reset(new StringTokenizer(input));
  tokenizer->GetLine();

  ASSERT_STREQ(tokenizer->GetTokenUnquoted(false), "&Symbol&");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(false), "&");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(false), "\\&");
  ASSERT_STREQ(tokenizer->GetTokenUnquoted(false), "escaped");
  ASSERT_EQ(tokenizer->GetTokenUnquoted(false), nullptr);
}

TEST(StringTokenizer, NextToken)
{
  std::string token;
  XrdOucString stoken;
  std::unique_ptr<StringTokenizer> tokenizer;

  std::string input = "Line to tokenize";
  tokenizer.reset(new StringTokenizer(input));

  // Parse using std::string token
  ASSERT_STREQ(tokenizer->GetLine(), "Line to tokenize");
  ASSERT_TRUE(tokenizer->NextToken(token));
  ASSERT_STREQ(token.c_str(), "Line");
  ASSERT_TRUE(tokenizer->NextToken(token));
  ASSERT_STREQ(token.c_str(), "to");
  ASSERT_TRUE(tokenizer->NextToken(token));
  ASSERT_STREQ(token.c_str(), "tokenize");
  ASSERT_FALSE(tokenizer->NextToken(token));

  tokenizer.reset(new StringTokenizer(input));

  // Parse using XrdOucString
  ASSERT_STREQ(tokenizer->GetLine(), "Line to tokenize");
  ASSERT_TRUE(tokenizer->NextToken(stoken));
  ASSERT_STREQ(stoken.c_str(), "Line");
  ASSERT_TRUE(tokenizer->NextToken(stoken));
  ASSERT_STREQ(stoken.c_str(), "to");
  ASSERT_TRUE(tokenizer->NextToken(stoken));
  ASSERT_STREQ(stoken.c_str(), "tokenize");
  ASSERT_FALSE(tokenizer->NextToken(stoken));
}

TEST(StringTokenizer, IsUnsignedNumber)
{
  // Valid numbers
  ASSERT_TRUE(StringTokenizer::IsUnsignedNumber("100"));
  ASSERT_TRUE(StringTokenizer::IsUnsignedNumber("0"));
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber("-100"));
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber("0100"));

  // Empty string
  std::string empty;
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber(""));
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber(empty));

  // Alphanumeric strings
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber("abc10"));
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber("10abc"));
  ASSERT_FALSE(StringTokenizer::IsUnsignedNumber("1bc1"));
}

EOSCOMMONTESTING_END
