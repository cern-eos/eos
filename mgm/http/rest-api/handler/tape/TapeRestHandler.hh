// ----------------------------------------------------------------------
// File: TapeRestHandler.hh
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

#ifndef EOS_TAPERESTHANDLER_HH
#define EOS_TAPERESTHANDLER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/handler/RestHandler.hh"
#include "mgm/http/rest-api/resources/ResourceFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

class TapeRestHandler : public RestHandler {
public:
  /**
   * Constructor of the TapeRestHandler
   * @param restApiUrl the base URL of the REST API without the instance name
   */
  TapeRestHandler(const std::string & restApiUrl);
  common::HttpResponse * handleRequest(common::HttpRequest * request) override;
  bool isRestRequest(const std::string & requestUrl) override;
private:
  inline static const std::string cEntryPointRegex = "^\\/([a-z0-9-]+)+\\/$";
  static void verifyRestApiEntryPoint(const std::string & restApiUrl);
  std::string mRestAPIUrl;
  std::unique_ptr<ResourceFactory> mResourceFactory;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTHANDLER_HH
