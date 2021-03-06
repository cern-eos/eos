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

#include "namespace/ns_quarkdb/inspector/FileMetadataFilter.hh"
#include "namespace/ns_quarkdb/inspector/AttributeExtraction.hh"
#include "common/Assert.hh"

EOSNSNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Empty constructor
//------------------------------------------------------------------------------
StringEvaluator::StringEvaluator()
: mName(), mLiteral(true) {}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
StringEvaluator::StringEvaluator(const std::string &name, bool literal)
: mName(name), mLiteral(literal) {}

//------------------------------------------------------------------------------
// Evaluate
//------------------------------------------------------------------------------
bool StringEvaluator::evaluate(const eos::ns::FileMdProto &proto, std::string &out) const {
  if(mLiteral) {
    out = mName;
    return true;
  }

  return AttributeExtraction::asString(proto, mName, out);
}

//------------------------------------------------------------------------------
// Describe
//------------------------------------------------------------------------------
std::string StringEvaluator::describe() const {
  if(mLiteral) return SSTR("'" << mName << "'");
  return mName;
}

//----------------------------------------------------------------------------
//! Constructor
//----------------------------------------------------------------------------
EqualityFileMetadataFilter::EqualityFileMetadataFilter(const StringEvaluator &ev1, const StringEvaluator &ev2, bool reverse)
: mEval1(ev1), mEval2(ev2), mReverse(reverse) {}

//----------------------------------------------------------------------------
// Does the given FileMdProto pass through the filter?
//----------------------------------------------------------------------------
bool EqualityFileMetadataFilter::check(const eos::ns::FileMdProto &proto) {
  std::string val1, val2;

  if(!mEval1.evaluate(proto, val1)) {
    return false;
  }

  if(!mEval2.evaluate(proto, val2)) {
    return false;
  }

  if(mReverse) {
    return val1 != val2;
  }
  else {
    return val1 == val2;
  }
}

//------------------------------------------------------------------------------
// Is the object valid?
//------------------------------------------------------------------------------
common::Status EqualityFileMetadataFilter::isValid() const {
  std::string tmp;
  eos::ns::FileMdProto proto;

  if(!mEval1.evaluate(proto, tmp)) {
    return common::Status(EINVAL, SSTR("could not evaluate string expression " << mEval1.describe()));
  }

  if(!mEval2.evaluate(proto, tmp)) {
    return common::Status(EINVAL, SSTR("could not evaluate string expression " << mEval2.describe()));
  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Describe object
//------------------------------------------------------------------------------
std::string EqualityFileMetadataFilter::describe() const {
  common::Status st = isValid();

  if(!st) {
    return SSTR("[" << st.toString() << "]");
  }

  if(mReverse) {
    return SSTR(mEval1.describe() << " != " << mEval2.describe());
  }

  return SSTR(mEval1.describe() << " == " << mEval2.describe());
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
LogicalMetadataFilter::LogicalMetadataFilter(std::unique_ptr<FileMetadataFilter> filt1,
    std::unique_ptr<FileMetadataFilter> filt2, bool isOr) {

  mFilter1 = std::move(filt1);
  mFilter2 = std::move(filt2);
  mIsOr = isOr;
}

//------------------------------------------------------------------------------
//! Is the object valid?
//------------------------------------------------------------------------------
common::Status LogicalMetadataFilter::isValid() const {
  if(!mFilter1->isValid()) return mFilter1->isValid();
  return mFilter2->isValid();
}

//------------------------------------------------------------------------------
// Does the given FileMdProto pass through the filter?
//------------------------------------------------------------------------------
bool LogicalMetadataFilter::check(const eos::ns::FileMdProto &proto) {
  bool firstCondition = mFilter1->check(proto);

  if(firstCondition && mIsOr) return true;
  if(!firstCondition && !mIsOr) return false;

  return mFilter2->check(proto);
}

//------------------------------------------------------------------------------
// Describe object
//------------------------------------------------------------------------------
std::string LogicalMetadataFilter::describe() const {
  if(mIsOr) {
    return SSTR("(" << mFilter1->describe() << " || " << mFilter2->describe() << ")");
  }

  return SSTR("(" << mFilter1->describe() << " && " << mFilter2->describe() << ")");
}

//------------------------------------------------------------------------------
// Lex the given string
//------------------------------------------------------------------------------
common::Status FilterExpressionLexer::lex(const std::string &str, std::vector<ExpressionLexicalToken> &tokens) {
  tokens.clear();

  size_t pos = 0;
  while(pos < str.size()) {
    if(str[pos] == '(') {
      tokens.emplace_back(ExpressionLexicalToken(TokenType::kLPAREN, "("));
      pos++;
      continue;
    }

    if(str[pos] == ')') {
      tokens.emplace_back(ExpressionLexicalToken(TokenType::kRPAREN, ")"));
      pos++;
      continue;
    }

    if(isspace(str[pos])) {
      pos++;
      continue;
    }

    if(str[pos] == '\'') {
      size_t initialPos = pos;
      pos++;

      while(true) {
        if(pos >= str.size()) {
          return common::Status(EINVAL, "lexing failed, mismatched quote: \"'\"");
        }

        if(str[pos] == '\'') {
          tokens.emplace_back(ExpressionLexicalToken(TokenType::kLITERAL, std::string(str.begin()+initialPos+1, str.begin()+pos)));
          break;
        }

        pos++;
      }

      pos++;
      continue;
    }

    if(str[pos] == '=') {
      pos++;

      if(pos >= str.size() || str[pos] != '=') {
        return common::Status(EINVAL, "lexing failed, single stray '=' found (did you mean '=='?)");
      }

      tokens.emplace_back(ExpressionLexicalToken(TokenType::kEQUALITY, "=="));

      pos++;
      continue;
    }

    if(str[pos] == '!') {
      pos++;

      if(pos >= str.size() || str[pos] != '=') {
        return common::Status(EINVAL, "lexing failed, single stray '!' found (did you mean '!='?)");
      }

      tokens.emplace_back(ExpressionLexicalToken(TokenType::kINEQUALITY, "!="));

      pos++;
      continue;
    }

    if(str[pos] == '&') {
      pos++;

      if(pos >= str.size() || str[pos] != '&') {
        return common::Status(EINVAL, "lexing failed, single stray '&' found (did you mean '&&'?)");
      }

      tokens.emplace_back(ExpressionLexicalToken(TokenType::kAND, "&&"));

      pos++;
      continue;
    }

    if(str[pos] == '|') {
      pos++;

      if(pos >= str.size() || str[pos] != '|') {
        return common::Status(EINVAL, "lexing failed, single stray '||' found (did you mean '||'?)");
      }

      tokens.emplace_back(ExpressionLexicalToken(TokenType::kOR, "||"));

      pos++;
      continue;
    }

    if(isalpha(str[pos])) {
      size_t initialPos = pos;

      while(true) {
        if(pos >= str.size() || isspace(str[pos])) {
          tokens.emplace_back(ExpressionLexicalToken(TokenType::kVAR, std::string(str.begin()+initialPos, str.begin()+pos)));
          break;
        }

        pos++;
      }
    }
    else {
      return common::Status(EINVAL, SSTR("Parse error, unreconized character: " << int(str[pos])));
    }

  }

  return common::Status();
}

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FilterExpressionParser::FilterExpressionParser(const std::string &str, bool showDebug)
: mDebug(showDebug) {

  mStatus = FilterExpressionLexer::lex(str, mTokens);
  mCurrent = 0;
  if(!mStatus) {
    fail(mStatus);
    return;
  }

  consumeBlock(mFilter);
}

//------------------------------------------------------------------------------
// Get status
//------------------------------------------------------------------------------
common::Status FilterExpressionParser::getStatus() const {
  return mStatus;
}

//------------------------------------------------------------------------------
// Get parsed filter -- call this only ONCE
//------------------------------------------------------------------------------
std::unique_ptr<FileMetadataFilter> FilterExpressionParser::getFilter() {
  return std::move(mFilter);
}

//------------------------------------------------------------------------------
// Accept token
//------------------------------------------------------------------------------
bool FilterExpressionParser::accept(TokenType type, ExpressionLexicalToken *tk) {
  if(mCurrent >= mTokens.size()) return false;
  if(mTokens[mCurrent].mType != type) return false;
  if(tk) *tk = mTokens[mCurrent];

  mCurrent++;
  return true;
}

//------------------------------------------------------------------------------
// Fail with the given error message
//------------------------------------------------------------------------------
bool FilterExpressionParser::fail(int errcode, const std::string &msg) {
  return fail(common::Status(errcode, msg));
}

//------------------------------------------------------------------------------
// Fail with the given status
//------------------------------------------------------------------------------
bool FilterExpressionParser::fail(const common::Status &st) {
  mStatus = st;
  mFilter.reset();
  return false;
}

//------------------------------------------------------------------------------
// Consume simple string expression
//------------------------------------------------------------------------------
bool FilterExpressionParser::consumeStringExpression(StringEvaluator &eval) {
  ExpressionLexicalToken token;
  if(accept(TokenType::kVAR, &token)) {
    eval = StringEvaluator(token.mContents, false);
    return true;
  }

  if(accept(TokenType::kLITERAL, &token)) {
    eval = StringEvaluator(token.mContents, true);
    return true;
  }

  return false;
}

//------------------------------------------------------------------------------
// Has next lexical token?
//------------------------------------------------------------------------------
bool FilterExpressionParser::hasNextToken() const {
  return mCurrent < mTokens.size();
}

//----------------------------------------------------------------------------
// Look-ahead token, but don't consume
//----------------------------------------------------------------------------
bool FilterExpressionParser::isLookahead(TokenType type) const {
  if(!hasNextToken()) return false;
  return mTokens[mCurrent].mType == type;
}

//------------------------------------------------------------------------------
// Consume parenthesied block
//------------------------------------------------------------------------------
bool FilterExpressionParser::consumeParenthesizedBlock(std::unique_ptr<FileMetadataFilter> &filter) {
  if(!accept(TokenType::kLPAREN)) {
    return fail(EINVAL, "expected '(' token");
  }

  if(!consumeBlock(filter)) {
    return false;
  }

  if(!accept(TokenType::kRPAREN)) {
    return fail(EINVAL, "expected ')' token");
  }

  return true;
}

//------------------------------------------------------------------------------
// Consume block
//------------------------------------------------------------------------------
bool FilterExpressionParser::consumeBlock(std::unique_ptr<FileMetadataFilter> &filter) {
  if(isLookahead(TokenType::kLPAREN)) {
    return consumeParenthesizedBlock(filter);
  }

  std::unique_ptr<FileMetadataFilter> leftSide;
  std::unique_ptr<FileMetadataFilter> rightSide;

  if(!consumeBooleanExpression(leftSide)) {
    return false;
  }

  if(!hasNextToken() || isLookahead(TokenType::kRPAREN)) {
    filter = std::move(leftSide);
    return true;
  }

  if(!accept(TokenType::kAND)) {
    return fail(EINVAL, "expected '&&' token");
  }

  if(!consumeBlock(rightSide)) {
    return false;
  }

  filter.reset(new LogicalMetadataFilter(std::move(leftSide), std::move(rightSide), false));
  return true;
}

//------------------------------------------------------------------------------
// Consume metadata filter
//------------------------------------------------------------------------------
bool FilterExpressionParser::consumeBooleanExpression(std::unique_ptr<FileMetadataFilter> &filter) {
  StringEvaluator eval1;
  StringEvaluator eval2;

  if(!consumeStringExpression(eval1)) {
    return fail(EINVAL, "expected string expression");
  }

  bool reversedEquality;

  if(accept(TokenType::kEQUALITY)) {
    reversedEquality = false;
  }
  else if(accept(TokenType::kINEQUALITY)) {
    reversedEquality = true;
  }
  else {
    return fail(EINVAL, "expected '==' or '!=' token");
  }

  if(!consumeStringExpression(eval2)) {
    return fail(EINVAL, "expected string expression");
  }

  filter.reset(new EqualityFileMetadataFilter(eval1, eval2, reversedEquality));
  return true;
}


EOSNSNAMESPACE_END

