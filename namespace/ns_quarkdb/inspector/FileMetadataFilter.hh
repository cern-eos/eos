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
//! @brief Class to filter out FileMDs
//------------------------------------------------------------------------------

#pragma once
#include "namespace/Namespace.hh"
#include "proto/FileMd.pb.h"
#include "common/Status.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
//! Class to decide whether to show a particular FileMD
//------------------------------------------------------------------------------
class FileMetadataFilter {
public:
  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~FileMetadataFilter() {}

  //----------------------------------------------------------------------------
  //! Is the object valid?
  //----------------------------------------------------------------------------
  virtual common::Status isValid() const = 0;

  //----------------------------------------------------------------------------
  //! Does the given FileMdProto pass through the filter?
  //----------------------------------------------------------------------------
  virtual bool check(const eos::ns::FileMdProto &proto) = 0;

  //----------------------------------------------------------------------------
  //! Describe object
  //----------------------------------------------------------------------------
  virtual std::string describe() const = 0;

};

//------------------------------------------------------------------------------
//! String evaluator
//------------------------------------------------------------------------------
class StringEvaluator {
public:
  //----------------------------------------------------------------------------
  //! Empty constructor
  //----------------------------------------------------------------------------
  StringEvaluator();

  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  StringEvaluator(const std::string &name, bool literal);

  //----------------------------------------------------------------------------
  //! Evaluate
  //----------------------------------------------------------------------------
  bool evaluate(const eos::ns::FileMdProto &proto, std::string &out) const;

  //----------------------------------------------------------------------------
  //! Describe
  //----------------------------------------------------------------------------
  std::string describe() const;

private:
  std::string mName;
  bool mLiteral;
};

//------------------------------------------------------------------------------
//! Filter which checks a particular FileMdProto attribute for equality
//------------------------------------------------------------------------------
class EqualityFileMetadataFilter : public FileMetadataFilter {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  EqualityFileMetadataFilter(const StringEvaluator &ev1, const StringEvaluator &ev2, bool reverse);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~EqualityFileMetadataFilter() {}

  //----------------------------------------------------------------------------
  //! Is the object valid?
  //----------------------------------------------------------------------------
  virtual common::Status isValid() const override;

  //----------------------------------------------------------------------------
  //! Does the given FileMdProto pass through the filter?
  //----------------------------------------------------------------------------
  virtual bool check(const eos::ns::FileMdProto &proto) override;

  //----------------------------------------------------------------------------
  //! Describe object
  //----------------------------------------------------------------------------
  virtual std::string describe() const override;

private:
  StringEvaluator mEval1;
  StringEvaluator mEval2;
  bool mReverse;
};

//------------------------------------------------------------------------------
//! && and || filter
//------------------------------------------------------------------------------
class LogicalMetadataFilter : public FileMetadataFilter {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  LogicalMetadataFilter(std::unique_ptr<FileMetadataFilter> filt1,
    std::unique_ptr<FileMetadataFilter> filt2, bool isOR);

  //----------------------------------------------------------------------------
  //! Destructor
  //----------------------------------------------------------------------------
  virtual ~LogicalMetadataFilter() {}

  //----------------------------------------------------------------------------
  //! Is the object valid?
  //----------------------------------------------------------------------------
  virtual common::Status isValid() const override;

  //----------------------------------------------------------------------------
  //! Does the given FileMdProto pass through the filter?
  //----------------------------------------------------------------------------
  virtual bool check(const eos::ns::FileMdProto &proto) override;

  //----------------------------------------------------------------------------
  //! Describe object
  //----------------------------------------------------------------------------
  virtual std::string describe() const override;

private:
  std::unique_ptr<FileMetadataFilter> mFilter1;
  std::unique_ptr<FileMetadataFilter> mFilter2;
  bool mIsOr;
};

//------------------------------------------------------------------------------
//! Token type
//------------------------------------------------------------------------------
enum class TokenType {
  kLPAREN, kRPAREN, kQUOTE, kLITERAL, kEQUALITY, kINEQUALITY, kAND, kOR, kVAR
};

struct ExpressionLexicalToken {
  TokenType mType;
  std::string mContents;

  ExpressionLexicalToken() {}

  ExpressionLexicalToken(TokenType t, std::string c) : mType(t), mContents(c) {}

  bool operator==(const ExpressionLexicalToken &other) const {
    return mType == other.mType && mContents == other.mContents;
  }
};

//------------------------------------------------------------------------------
//! Filter expression lexer
//------------------------------------------------------------------------------
class FilterExpressionLexer {
public:
  //----------------------------------------------------------------------------
  //! Lex the given string
  //----------------------------------------------------------------------------
  static common::Status lex(const std::string &str, std::vector<ExpressionLexicalToken> &tokens);
};

//------------------------------------------------------------------------------
//! Filter expression parser
//------------------------------------------------------------------------------
class FilterExpressionParser {
public:
  //----------------------------------------------------------------------------
  //! Constructor
  //----------------------------------------------------------------------------
  FilterExpressionParser(const std::string &str, bool showDebug);

  //----------------------------------------------------------------------------
  //! Get status
  //----------------------------------------------------------------------------
  common::Status getStatus() const;

  //----------------------------------------------------------------------------
  //! Get parsed filter -- call this only ONCE
  //----------------------------------------------------------------------------
  std::unique_ptr<FileMetadataFilter> getFilter();

private:
  //----------------------------------------------------------------------------
  //! Accept token
  //----------------------------------------------------------------------------
  bool accept(TokenType type, ExpressionLexicalToken *token = nullptr);

  //----------------------------------------------------------------------------
  //! Look-ahead token, but don't consume
  //----------------------------------------------------------------------------
  bool isLookahead(TokenType type) const;

  //----------------------------------------------------------------------------
  //! Has next lexical token?
  //----------------------------------------------------------------------------
  bool hasNextToken() const;

  //----------------------------------------------------------------------------
  // Consume parenthesied block
  //----------------------------------------------------------------------------
  bool consumeParenthesizedBlock(std::unique_ptr<FileMetadataFilter> &filter);

  //----------------------------------------------------------------------------
  //! Consume block
  //----------------------------------------------------------------------------
  bool consumeBlock(std::unique_ptr<FileMetadataFilter> &filter);

  //----------------------------------------------------------------------------
  //! Consume metadata filter
  //----------------------------------------------------------------------------
  bool consumeBooleanExpression(std::unique_ptr<FileMetadataFilter> &filter);

  //----------------------------------------------------------------------------
  //! Consume simple string expression
  //----------------------------------------------------------------------------
  bool consumeStringExpression(StringEvaluator &eval);

  //----------------------------------------------------------------------------
  //! Fail with the given status
  //----------------------------------------------------------------------------
  bool fail(const common::Status &st);

  //----------------------------------------------------------------------------
  //! Fail with the given status
  //----------------------------------------------------------------------------
  bool fail(int errcode, const std::string &msg);

  std::vector<ExpressionLexicalToken> mTokens;
  size_t mCurrent;

  common::Status mStatus;
  bool mDebug;
  std::unique_ptr<FileMetadataFilter> mFilter;
};



EOSNSNAMESPACE_END
