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
#include <regex>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * Class allowing to parse the URL given via the constructor
 * and extract information depending on a pattern
 *
 * The URL pattern should have the following format: /api/v1/stage/{requestid}/cancel
 * the {requestid} is actually a placeholder that this parser will rely on to extract parameters
 * from the client url
 */
class URLParser
{
public:
  URLParser(const std::string& url);
  /**
   * Returns true if the URL of this instance
   * starts by the URL passed in parameter
   * @param url the URL to compare this instance URL with
   * @return true the URL of this instance
   * starts by the URL passed in parameter, false otherwise
   */
  bool startsBy(const std::string& url);
  /**
   * Returns true if the URL of this instance matches exactly
   * the URL pattern passed in parameter, false otherwise
   * @param urlPattern the pattern the URL should match
   * @return true if the URL of this instance matches the url pattern, false otherwise
   */
  bool matches(const std::string& urlPattern);

  /**
   * Returns true if the URL of this instance matches exactly the URL
   * pattern passed in paramter, false otherwise.
   * It also extracts the values located at the placeholders
   * place in this instance URL.
   *
   * Example:
   *    thisURL = /api/v1/stage/xxx-xxx/cancel
   *    urlPattern=/api/v1/stage/{requestid}/cancel
   *    This method will return true and the params map will contain "requestid":"xxx-xxx"
   */
  bool matchesAndExtractParameters(const std::string& urlPattern,
                                   std::map<std::string, std::string>& params);

  /**
   * Removes the duplicate slashes from a path given in parameter
   * example: /eos//test/////file.txt --> /eos/test/file.txt
   * @param path the path to remove the duplicate slashes from
   */
  static void removeDuplicateSlashes(std::string& path);

private:
  std::vector<std::string> mURLTokens;
  static const std::regex cParamRegex;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_URLPARSER_HH
