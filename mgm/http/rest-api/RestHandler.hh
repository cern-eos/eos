// ----------------------------------------------------------------------
// File: RestHandler.hh
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

#ifndef EOS_RESTHANDLER_HH
#define EOS_RESTHANDLER_HH

#include "mgm/Namespace.hh"
#include <memory>
#include "common/http/HttpResponse.hh"
#include "common/http/HttpRequest.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * This class allows to handle REST requests.
 */
class RestHandler {
public:
  /**
   * Constructor of the RestHandler
   * @param restApiUrl the base URL of the REST API without the instance name
   */
  RestHandler(const std::string & restApiUrl);
  common::HttpResponse * handleRequest(common::HttpRequest * request);
  bool isRestRequest(const std::string & requestUrl);
private:
  const std::string extractResourceFromUrl(const std::string & url);
  const std::string extractVersionFromUrl(const std::string & url);

  const std::string mRestAPIUrl;
};

EOSMGMNAMESPACE_END

#endif // EOS_RESTHANDLER_HH
