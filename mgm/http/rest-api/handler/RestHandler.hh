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

#include <regex>
#include <memory>
#include <map>

#include "mgm/Namespace.hh"
#include "common/http/HttpResponse.hh"
#include "common/http/HttpRequest.hh"
#include "common/VirtualIdentity.hh"
#include <XrdHttp/XrdHttpExtHandler.hh>
#include "mgm/http/rest-api/controllers/ControllerManager.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class allows to handle REST requests.
 */
class RestHandler
{
public:
  RestHandler(const std::string& entryPointURL);
  /**
   * Handles the request given in parameter and returns a response
   * @param request the request to handle
   * @return A pointer to an HttpResponse object that will contain the JSON response that will be returned to the client
   */
  virtual common::HttpResponse* handleRequest(common::HttpRequest* request,
      const common::VirtualIdentity* vid) = 0;

  /**
   * Returns true if the requestURL passed in parameter should trigger an API handling,
   * false otherwise
   * @param requestUrl the user request URL
   * @param errorMsg the error message that one can use to indicate why the call to the REST API will not have any action
   * @return true if the requestURL passed in parameter should trigger an API handling,
   * false otherwise
   */
  virtual bool isRestRequest(const std::string& requestURL,
                             std::string& errorMsg) const;

  /**
   * @return the RestHandler entry point URL (example: /api/)
   */
  std::string getEntryPointURL() const;

  virtual ~RestHandler() {}
protected:
  ControllerManager mControllerManager;
  std::string mEntryPointURL;
private:
  static const std::regex cEntryPointRegex;
  void verifyRestApiEntryPoint(const std::string& entryPointURL) const;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTHANDLER_HH
