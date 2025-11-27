// ----------------------------------------------------------------------
// File: RestHandler.cc
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

#include "RestHandler.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "common/Logging.hh"
#include "common/RegexWrapper.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"

namespace
{
std::string sEntryPointRegex("^\\/(\\.?[a-z0-9-]+)+\\/$");
}

EOSMGMRESTNAMESPACE_BEGIN

RestHandler::RestHandler(const std::string& entryPointURL): mEntryPointURL(
    entryPointURL)
{
  verifyRestApiEntryPoint(entryPointURL);
}

bool RestHandler::isRestRequest(const std::string& requestUrl,
                                std::string& errorMsg) const
{
  //The URL should start with the API entry URL
  URLParser parser(requestUrl);
  return parser.startsBy(mEntryPointURL);
}

void RestHandler::verifyRestApiEntryPoint(const std::string& entryPointURL)
const
{
  if (!eos::common::eos_regex_match(entryPointURL, sEntryPointRegex)) {
    std::stringstream ss;
    ss << "The REST API entrypoint provided (" << entryPointURL <<
       ") is malformed. It should have the format: /apientrypoint/.";
    throw RestException(ss.str());
  }
}

std::string RestHandler::getEntryPointURL() const
{
  return mEntryPointURL;
}

EOSMGMRESTNAMESPACE_END
