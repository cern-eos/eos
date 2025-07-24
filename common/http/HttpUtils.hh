// ----------------------------------------------------------------------
// File: HttpUtils.hh
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


#ifndef EOS_HTTPUTILS_H
#define EOS_HTTPUTILS_H

#include "common/Namespace.hh"
#include "common/StringTokenizer.hh"
#include "common/StringUtils.hh"
#include <string>
#include <vector>
#include <map>

EOSCOMMONNAMESPACE_BEGIN

class HttpUtils {
public:
  static void parseReprDigest(const std::string &header, std::map<std::string, std::string> &output);
};

EOSCOMMONNAMESPACE_END
#endif // EOS_HTTPUTILS_H
