// ----------------------------------------------------------------------
// File: HttpUtils.cc
// Author: Cedric Caffy - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2025 CERN/Switzerland                                  *
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

#include "HttpUtils.hh"

EOSCOMMONNAMESPACE_BEGIN

void HttpUtils::parseReprDigest(const std::string& header, std::map<std::string, std::string>& output) {
  // Expected format per entry: <cksumType>=:<digestValue>:
  std::vector<std::string> digestNameValuePairs = common::StringTokenizer::split<std::vector<std::string>>(header,',');

  for (const auto& digestNameValue : digestNameValuePairs) {
    std::string_view digestNameValueSV{digestNameValue};
    auto equalPos = digestNameValueSV.find('=');
    if (equalPos == std::string::npos ||
        equalPos >= digestNameValueSV.size() - 1)
      continue;

    std::string_view cksumTypeSV = digestNameValueSV.substr(0, equalPos);
    common::trim(cksumTypeSV);
    if (cksumTypeSV.empty())
      continue;

    std::string_view cksumValueInSV = digestNameValueSV.substr(equalPos + 1);
    size_t beginCksumPos = cksumValueInSV.find(':');
    size_t endCksumPos = cksumValueInSV.rfind(':');

    // Check that the string starts with ':' and contains two distinct colons
    if (beginCksumPos == 0 && endCksumPos > beginCksumPos + 1 &&
        endCksumPos < cksumValueInSV.size()) {
      std::string_view cksumValue = cksumValueInSV.substr(
          beginCksumPos + 1, endCksumPos - beginCksumPos - 1);
      common::trim(cksumValue);
      if (!cksumValue.empty())
        output[std::string(cksumTypeSV)] = cksumValue;
    }
    // Malformed entries are silently ignored
  }
}

EOSCOMMONNAMESPACE_END