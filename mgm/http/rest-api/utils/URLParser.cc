// ----------------------------------------------------------------------
// File: URLParser.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2013 CERN/Switzerland                                  *
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
#include "URLParser.hh"
#include "common/StringConversion.hh"
#include <algorithm>

EOSMGMRESTNAMESPACE_BEGIN

const std::regex URLParser::cParamRegex("^\\{[a-z]*\\}$");

URLParser::URLParser(const std::string& url)
{
  common::StringConversion::Tokenize(url, mURLTokens, "/");
}

bool URLParser::startsBy(const std::string& url)
{
  std::vector<std::string> urlTokens;
  common::StringConversion::Tokenize(url, urlTokens, "/");
  uint8_t urlTokensSize = urlTokens.size();

  if (mURLTokens.size() < urlTokensSize) {
    return false;
  }

  for (uint8_t i = 0; i < urlTokensSize; ++i) {
    if (urlTokens.at(i) != mURLTokens.at(i)) {
      return false;
    }
  }

  return true;
}

bool URLParser::matches(const std::string& urlPattern)
{
  std::map<std::string, std::string> params;
  return matchesAndExtractParameters(urlPattern, params);
}

bool URLParser::matchesAndExtractParameters(const std::string& urlPattern,
    std::map<std::string, std::string>& params)
{
  params.clear();
  std::vector<std::string> urlPatternTokens;
  common::StringConversion::Tokenize(urlPattern, urlPatternTokens, "/");

  if (mURLTokens.size() != urlPatternTokens.size()) {
    return false;
  }

  uint8_t urlPatternTokensSize = urlPatternTokens.size();

  for (uint8_t i = 0; i < urlPatternTokensSize; ++i) {
    const std::string& urlPatternToken = urlPatternTokens.at(i);
    const std::string& urlToken = mURLTokens.at(i);

    if (urlToken != urlPatternToken) {
      //URL parts do not match, maybe it is a parameter, try to extract it
      std::smatch urlParamMatch;

      if (std::regex_match(urlPatternToken, urlParamMatch, cParamRegex)) {
        params[urlParamMatch[0]] = urlToken;
      } else {
        return false;
      }
    }
  }

  return true;
}

void URLParser::removeDuplicateSlashes(std::string& path)
{
  path.erase(std::unique(path.begin(), path.end(), [](char a, char b) {
    return a == '/' && b == '/';
  }), path.end());
}

EOSMGMRESTNAMESPACE_END
