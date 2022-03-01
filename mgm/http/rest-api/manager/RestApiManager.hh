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
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"
#include "mgm/http/rest-api/handler/factory/TapeRestHandlerFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

class RestApiManager {
public:
  RestApiManager();
  virtual bool isRestRequest(const std::string & requestURL);
  virtual TapeRestApiConfig * getTapeRestApiConfig();
  virtual std::unique_ptr<rest::RestHandler> getRestHandler(const std::string & requestURL);
  virtual ~RestApiManager(){}
private:
  std::unique_ptr<TapeRestApiConfig> mTapeRestApiConfig;
  std::map<std::string,std::unique_ptr<RestHandlerFactory>> mMapAccessURLRestHandlerFactory;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTAPIMANAGER_HH
