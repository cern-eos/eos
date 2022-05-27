// ----------------------------------------------------------------------
// File: WellKnownHandler.hh
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

#ifndef EOS_WELLKNOWNHANDLER_HH
#define EOS_WELLKNOWNHANDLER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/handler/RestHandler.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/http/rest-api/response/wellknown/WellKnownResponseFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

class WellKnownHandler : public RestHandler
{
public:
  WellKnownHandler(const std::string& accessURL,
                   const RestApiManager* restApiManager);
  /**
   * Handles the user request
   * @param request the user request
   * @param vid the virtual identity of the user
   * @return the HttpResponse to the user request
   */
  common::HttpResponse* handleRequest(common::HttpRequest* request,
                                      const common::VirtualIdentity* vid) override;

private:
  void initializeControllers();
  const RestApiManager* mRestApiManager;
  WellKnownResponseFactory mWellknownResponseFactory;
};

EOSMGMRESTNAMESPACE_END
#endif // EOS_WELLKNOWNHANDLER_HH
