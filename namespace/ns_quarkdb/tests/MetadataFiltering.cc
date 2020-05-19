/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2020 CERN/Switzerland                                  *
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

//------------------------------------------------------------------------------
//! @author Georgios Bitzes <georgios.bitzes@cern.ch>
//! @brief Metadata filtering tests
//------------------------------------------------------------------------------

#include "namespace/ns_quarkdb/inspector/FileMetadataFilter.hh"
#include "namespace/ns_quarkdb/inspector/AttributeExtraction.hh"
#include "common/LayoutId.hh"

#include <gtest/gtest.h>

using namespace eos;

TEST(AttributeExtraction, BasicSanity) {
  eos::ns::FileMdProto proto;
  std::string out;

  ASSERT_FALSE(AttributeExtraction::asString(proto, "aaa", out));

  ASSERT_TRUE(AttributeExtraction::asString(proto, "xattr.aaa", out));
  ASSERT_TRUE(out.empty());

  (*proto.mutable_xattrs())["user.test"] = "123";
  ASSERT_TRUE(AttributeExtraction::asString(proto, "xattr.user.test", out));
  ASSERT_EQ(out, "123");

  proto.set_id(1111);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "fid", out));
  ASSERT_EQ(out, "1111");

  proto.set_cont_id(22222);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "pid", out));
  ASSERT_EQ(out, "22222");

  proto.set_gid(333);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "gid", out));
  ASSERT_EQ(out, "333");

  proto.set_uid(444);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "uid", out));
  ASSERT_EQ(out, "444");

  proto.set_size(555);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "size", out));
  ASSERT_EQ(out, "555");

  unsigned long layout = eos::common::LayoutId::GetId(
    eos::common::LayoutId::kReplica,
    eos::common::LayoutId::kAdler,
    2,
    eos::common::LayoutId::k4k);

  proto.set_layout_id(layout);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "layout_id", out));
  ASSERT_EQ(out, "1048850");

  proto.set_flags(0777);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "flags", out));
  ASSERT_EQ(out, "777");

  proto.set_name("aaaaa");
  ASSERT_TRUE(AttributeExtraction::asString(proto, "name", out));
  ASSERT_EQ(out, "aaaaa");

  proto.set_link_name("bbbbbb");
  ASSERT_TRUE(AttributeExtraction::asString(proto, "link_name", out));
  ASSERT_EQ(out, "bbbbbb");

  struct timespec ctime;
  ctime.tv_sec = 1999;
  ctime.tv_nsec = 8888;
  proto.set_ctime(&ctime, sizeof(ctime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "ctime", out));
  ASSERT_EQ(out, "1999.8888");

  struct timespec mtime;
  mtime.tv_sec = 1998;
  mtime.tv_nsec = 7777;
  proto.set_mtime(&mtime, sizeof(mtime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "mtime", out));
  ASSERT_EQ(out, "1998.7777");

  char buff[32];
  buff[0] = 0x12; buff[1] = 0x23; buff[2] = 0x55; buff[3] = 0x99;
  buff[4] = 0xAA; buff[5] = 0xDD; buff[6] = 0x00; buff[7] = 0x55;
  proto.set_checksum(buff, 8);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "xs", out));
  ASSERT_EQ(out, "12235599");

  proto.add_locations(3);
  proto.add_locations(2);
  proto.add_locations(1);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "locations", out));
  ASSERT_EQ(out, "3,2,1");

  proto.add_unlink_locations(4);
  proto.add_unlink_locations(5);
  proto.add_unlink_locations(6);
  ASSERT_TRUE(AttributeExtraction::asString(proto, "unlink_locations", out));
  ASSERT_EQ(out, "4,5,6");

  struct timespec stime;
  stime.tv_sec = 1997;
  stime.tv_nsec = 5555;
  proto.set_stime(&stime, sizeof(stime));
  ASSERT_TRUE(AttributeExtraction::asString(proto, "stime", out));
  ASSERT_EQ(out, "1997.5555");
}

TEST(FileMetadataFilter, InvalidFilter) {
  EqualityFileMetadataFilter invalidFilter("invalid.attr", "aaa");
  ASSERT_FALSE(invalidFilter.isValid());
  ASSERT_EQ(invalidFilter.describe(), "[(22): Unknown FileMD attribute: invalid.attr]");
}

TEST(FileMetadataFilter, ZeroSizeFilter) {
  EqualityFileMetadataFilter sizeFilter("size", "0");
  ASSERT_TRUE(sizeFilter.isValid());
  ASSERT_EQ(sizeFilter.describe(), "size == '0'");

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_FALSE(sizeFilter.check(proto));

  proto.set_size(0);
  ASSERT_TRUE(sizeFilter.check(proto));
}

TEST(FileMetadataFilter, ParsedExpressionFilter) {
  std::unique_ptr<FileMetadataFilter> sub(new EqualityFileMetadataFilter("size", "0"));
  ParsedFileMetadataFilter parsedFilter(std::move(sub));

  ASSERT_TRUE(parsedFilter.isValid());
  ASSERT_EQ(parsedFilter.describe(), "size == '0'");

  eos::ns::FileMdProto proto;
  proto.set_size(33);
  ASSERT_FALSE(parsedFilter.check(proto));

  proto.set_size(0);
  ASSERT_TRUE(parsedFilter.check(proto));

  parsedFilter = ParsedFileMetadataFilter(common::Status(EINVAL, "invalid expression 'abc'"));
  ASSERT_FALSE(parsedFilter.isValid());
  ASSERT_EQ(parsedFilter.describe(), "[failed to parse expression: (22): invalid expression 'abc'");
  ASSERT_FALSE(parsedFilter.check(proto));
}

TEST(FilterExpressionLexer, BasicSanity) {
  std::vector<ExpressionLexicalToken> tokens;
  common::Status st = FilterExpressionLexer::lex("   (  'abc )( ' == ' cde' && || ) ", tokens);

  ASSERT_TRUE(st);
  ASSERT_EQ(tokens.size(), 7u);

  ASSERT_EQ(tokens[0], ExpressionLexicalToken(TokenType::kLPAREN, "("));
  ASSERT_EQ(tokens[1], ExpressionLexicalToken(TokenType::kLITERAL, "abc )( "));
  ASSERT_EQ(tokens[2], ExpressionLexicalToken(TokenType::kEQUALITY, "=="));
  ASSERT_EQ(tokens[3], ExpressionLexicalToken(TokenType::kLITERAL, " cde"));
  ASSERT_EQ(tokens[4], ExpressionLexicalToken(TokenType::kAND, "&&"));
  ASSERT_EQ(tokens[5], ExpressionLexicalToken(TokenType::kOR, "||"));
  ASSERT_EQ(tokens[6], ExpressionLexicalToken(TokenType::kRPAREN, ")"));
}

TEST(FilterExpressionLexer, VariableEquality) {
  std::vector<ExpressionLexicalToken> tokens;
  common::Status st = FilterExpressionLexer::lex("   ( varName123 == 'abc' ) ", tokens);

  ASSERT_TRUE(st);
  ASSERT_EQ(tokens.size(), 5u);

  ASSERT_EQ(tokens[0], ExpressionLexicalToken(TokenType::kLPAREN, "("));

  ASSERT_EQ(tokens[1].mType, TokenType::kVAR);
  ASSERT_EQ(tokens[1].mContents, "varName123");

  ASSERT_EQ(tokens[1], ExpressionLexicalToken(TokenType::kVAR, "varName123"));

  ASSERT_EQ(tokens[2], ExpressionLexicalToken(TokenType::kEQUALITY, "=="));
  ASSERT_EQ(tokens[3], ExpressionLexicalToken(TokenType::kLITERAL, "abc"));
  ASSERT_EQ(tokens[4], ExpressionLexicalToken(TokenType::kRPAREN, ")"));
}

TEST(FilterExpressionLexer, MismatchedQuote) {
  std::vector<ExpressionLexicalToken> tokens;
  common::Status st = FilterExpressionLexer::lex("     'abc )(  ) ", tokens);

  ASSERT_FALSE(st);
  ASSERT_EQ(st.toString(), "(22): lexing failed, mismatched quote: \"'\"");
}
