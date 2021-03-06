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

TEST(StringEvaluator, Literal) {
  eos::ns::FileMdProto proto;
  std::string out;

  StringEvaluator literal("some string literal", true);
  ASSERT_EQ(literal.describe(), "'some string literal'");
  ASSERT_TRUE(literal.evaluate(proto, out));
  ASSERT_EQ(out, "some string literal");

  literal = StringEvaluator("", true);
  ASSERT_EQ(literal.describe(), "''");
  ASSERT_TRUE(literal.evaluate(proto, out));
  ASSERT_EQ(out, "");
}

TEST(StringEvaluator, VariableName) {
  eos::ns::FileMdProto proto;
  std::string out;

  proto.set_size(5);

  StringEvaluator literal("size", false);
  ASSERT_EQ(literal.describe(), "size");
  ASSERT_TRUE(literal.evaluate(proto, out));
  ASSERT_EQ(out, "5");

  proto.set_size(555);
  ASSERT_TRUE(literal.evaluate(proto, out));
  ASSERT_EQ(out, "555");
}

TEST(StringEvaluator, InvalidVariableName) {
  eos::ns::FileMdProto proto;
  std::string out;

  StringEvaluator literal("aaa", false);
  ASSERT_EQ(literal.describe(), "aaa");
  ASSERT_FALSE(literal.evaluate(proto, out));
  ASSERT_EQ(out, "");
}

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
  EqualityFileMetadataFilter invalidFilter(
    StringEvaluator("invalid.attr", false), StringEvaluator("aaa", true), false);

  ASSERT_FALSE(invalidFilter.isValid());
  ASSERT_EQ(invalidFilter.describe(), "[(22): could not evaluate string expression invalid.attr]");
}

TEST(FileMetadataFilter, ZeroSizeFilter) {
  EqualityFileMetadataFilter sizeFilter(StringEvaluator("size", false), StringEvaluator("0", true), false);
  ASSERT_TRUE(sizeFilter.isValid());
  ASSERT_EQ(sizeFilter.describe(), "size == '0'");

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_FALSE(sizeFilter.check(proto));

  proto.set_size(0);
  ASSERT_TRUE(sizeFilter.check(proto));
}

TEST(FileMetadataFilter, AndFilter) {
  std::unique_ptr<FileMetadataFilter> sizeFilter(
    new EqualityFileMetadataFilter(StringEvaluator("size", false), StringEvaluator("0", true), false));

  std::unique_ptr<FileMetadataFilter> nameFilter(
    new EqualityFileMetadataFilter(StringEvaluator("name", false), StringEvaluator("chickens", true), false));

  LogicalMetadataFilter andFilter(
    std::move(sizeFilter),
    std::move(nameFilter),
    false
  );

  ASSERT_EQ(andFilter.describe(), "(size == '0' && name == 'chickens')");

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_FALSE(andFilter.check(proto));

  proto.set_size(0);
  ASSERT_FALSE(andFilter.check(proto));

  proto.set_name("chickens");
  ASSERT_TRUE(andFilter.check(proto));

  proto.set_name("chickens-2");
  ASSERT_FALSE(andFilter.check(proto));
}

TEST(FileMetadataFilter, OrFilter) {
  std::unique_ptr<FileMetadataFilter> sizeFilter(
    new EqualityFileMetadataFilter(StringEvaluator("size", false), StringEvaluator("0", true), false));

  std::unique_ptr<FileMetadataFilter> nameFilter(
    new EqualityFileMetadataFilter(StringEvaluator("name", false), StringEvaluator("chickens", true), false));

  LogicalMetadataFilter orFilter(
    std::move(sizeFilter),
    std::move(nameFilter),
    true
  );

  ASSERT_EQ(orFilter.describe(), "(size == '0' || name == 'chickens')");

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_FALSE(orFilter.check(proto));

  proto.set_size(0);
  ASSERT_TRUE(orFilter.check(proto));

  proto.set_name("chickens");
  ASSERT_TRUE(orFilter.check(proto));

  proto.set_name("chickens-2");
  ASSERT_TRUE(orFilter.check(proto));

  proto.set_size(22);
  ASSERT_FALSE(orFilter.check(proto));
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

TEST(FilterExpressionParser, SimpleEquality) {
  FilterExpressionParser parser("size == '0'", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "size == '0'");
  ASSERT_TRUE(filter->isValid());

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_FALSE(filter->check(proto));

  proto.set_size(0);
  ASSERT_TRUE(filter->check(proto));
}

TEST(FilterExpressionParser, YodaEquality) {
  FilterExpressionParser parser("'0' == size", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "'0' == size");
  ASSERT_TRUE(filter->isValid());
}

TEST(FilterExpressionParser, LiteralEquality) {
  FilterExpressionParser parser("'0' == '1'", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "'0' == '1'");
  ASSERT_TRUE(filter->isValid());
}

TEST(FilterExpressionParser, AndExpression) {
  FilterExpressionParser parser("size == '0' && name == 'chickens'", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "(size == '0' && name == 'chickens')");
  ASSERT_TRUE(filter->isValid());
}

TEST(FilterExpressionParser, TripleAndExpression) {
  FilterExpressionParser parser("size == '0' && name == 'chickens' && pid == '0'", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "(size == '0' && (name == 'chickens' && pid == '0'))");
  ASSERT_TRUE(filter->isValid());
}

TEST(FilterExpressionParser, NotEquals) {
  FilterExpressionParser parser("size != '0'", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "size != '0'");
  ASSERT_TRUE(filter->isValid());

  eos::ns::FileMdProto proto;

  proto.set_size(33);
  ASSERT_TRUE(filter->check(proto));

  proto.set_size(0);
  ASSERT_FALSE(filter->check(proto));
}

TEST(FilterExpressionParser, EqualityWithParentheses) {
  FilterExpressionParser parser("(size == '0')", true);
  ASSERT_TRUE(parser.getStatus());

  std::unique_ptr<FileMetadataFilter> filter = parser.getFilter();
  ASSERT_EQ(filter->describe(), "size == '0'");
  ASSERT_TRUE(filter->isValid());
}
