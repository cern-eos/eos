// ----------------------------------------------------------------------
// File: RestApiManager.hh
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

#ifndef EOS_RESTAPIMANAGER_HH
#define EOS_RESTAPIMANAGER_HH

#include <string>
#include <memory>
#include <map>
#include <functional>
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/handler/RestHandler.hh"
#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class is responsible for managing all the REST API
 * that the EOS instance is running
 */
class RestApiManager
{
public:
  RestApiManager();
  /**
   * Returns true if the request URL maps to
   * a specific REST Handler and if the REST handler
   * accepts requests.
   * @param requestURL the URL provided by the client
   */
  virtual bool isRestRequest(const std::string& requestURL) const;
  /**
   * Returns the tape REST API configuration object hold by this
   * manager
   * Use this method to access the configuration but also to modify its content
   * @return a pointer to the tape REST API configuration object
   */
  virtual TapeRestApiConfig* getTapeRestApiConfig() const;
  /**
   * Instanciate a RestHandler depending on the request URL provided
   * @param requestURL the URL of the client's request
   * @return a unique_ptr pointing to an instaciated RestHandler, nullptr if no RestHandler
   * matches the requestURL
   */
  virtual std::unique_ptr<rest::RestHandler> getRestHandler(
    const std::string& requestURL) const;
  /**
   * @return the .well-known endpoint access URL
   */
  virtual const std::string getWellKnownAccessURL() const;

  virtual ~RestApiManager() {}

private:
  //The Tape REST API configuration object
  std::unique_ptr<TapeRestApiConfig> mTapeRestApiConfig;
  //A map of <URL, RestHandlerCreator>
  std::map<std::string, std::function<std::unique_ptr<rest::RestHandler>()>>
      mMapAccessURLRestHandlerCreator;
  //URL of the wellknown access URL
  std::string mWellKnownAccessURL;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTAPIMANAGER_HH
