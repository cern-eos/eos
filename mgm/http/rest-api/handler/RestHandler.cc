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
#include <regex>
#include "mgm/http/rest-api/exception/RestException.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestHandler::RestHandler(const std::string& entryPointURL):mEntryPointURL(entryPointURL){
  verifyRestApiEntryPoint(entryPointURL);
}

bool RestHandler::isRestRequest(const std::string& requestUrl){
  //The URL should start with the API entry URL
  return ::strncmp(mEntryPointURL.c_str(),requestUrl.c_str(),mEntryPointURL.length()) == 0;
}

void RestHandler::verifyRestApiEntryPoint(const std::string & entryPointURL) {
  std::regex entryPointRegex(cEntryPointRegex);
  if(!std::regex_match(entryPointURL,entryPointRegex)){
    std::stringstream ss;
    ss << "The REST API entrypoint provided (" << entryPointURL << ") is malformed. It should have the format: /apientrypoint/.";
    throw RestException(ss.str());
  }
}

EOSMGMRESTNAMESPACE_END
