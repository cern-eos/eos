//------------------------------------------------------------------------------
//! @file RouteEndpoint.cc
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2018 CERN/Switzerland                                  *
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
 * You should have received a copy of the AGNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include "mgm/RouteEndpoint.hh"
#include "common/StringConversion.hh"
#include <sstream>

EOSMGMNAMESPACE_BEGIN

//------------------------------------------------------------------------------
// Parse route endpoint specification from string
//------------------------------------------------------------------------------
bool
RouteEndpoint::ParseFromString(const std::string& input)
{
  std::vector<std::string> tokens;
  eos::common::StringConversion::Tokenize(input, tokens, ":");

  if (tokens.size() != 3) {
    return false;
  }

  mFqdn = tokens[0];

  try {
    mXrdPort = std::stoul(tokens[1]);
    mHttpPort = std::stoul(tokens[2]);
  } catch (const std::exception& e) {
    return false;
  }

  return true;
}

//------------------------------------------------------------------------------
// Get string representation of the endpoint
//------------------------------------------------------------------------------
std::string
RouteEndpoint::ToString() const
{
  std::ostringstream oss;
  oss << mFqdn << ":" << mXrdPort << ":" << mHttpPort;
  return oss.str();
}

EOSMGMNAMESPACE_END
