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
#include "mgm/http/rest-api/business/tape/ITapeRestApiBusiness.hh"
#include "mgm/http/rest-api/controllers/Controller.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class handles the HTTP requests that are
 * intended for the WLCG TAPE REST API
 */
class TapeRestHandler : public RestHandler {
public:
  /**
   * Constructor of the TapeRestHandler
   * @param restApiUrl the base URL of the REST API without the instance name
   */
  TapeRestHandler(const std::string & entryPointURL = "/api/");
  common::HttpResponse * handleRequest(common::HttpRequest * request, const common::VirtualIdentity * vid) override;
private:
  void initializeControllers();
  std::unique_ptr<Controller> initializeStageController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);
  std::unique_ptr<Controller> initializeFileInfoController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);
  TapeRestApiResponseFactory mTapeRestApiResponseFactory;
  inline static const std::string VERSION_0 = "v1";
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTHANDLER_HH
