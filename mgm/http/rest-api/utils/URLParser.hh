// ----------------------------------------------------------------------
// File: URLParser.hh
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
#ifndef EOS_URLPARSER_HH
#define EOS_URLPARSER_HH

#include "mgm/Namespace.hh"
#include <string>
#include <vector>
#include <map>

EOSMGMRESTNAMESPACE_BEGIN

class URLParser {
public:
  URLParser(const std::string & url);
  /**
   * Returns true if the URL of this instance
   * starts by the URL passed in parameter
   * @param url the URL to compare this instance URL with
   * @return true the URL of this instance
   * starts by the URL passed in parameter, false otherwise
   */
  bool startsBy(const std::string & url);
  bool matches(const std::string & urlPattern);
  bool matchesAndExtractParameters(const std::string & urlPattern, std::map<std::string, std::string>& params);
private:
  const std::string & mURL;
  std::vector<std::string> mURLTokens;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_URLPARSER_HH
