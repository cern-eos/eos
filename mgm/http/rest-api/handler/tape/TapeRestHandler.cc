// ----------------------------------------------------------------------
// File: TapeRestHandler.cc
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

#include "TapeRestHandler.hh"
#include "common/StringConversion.hh"
#include "common/http/HttpServer.hh"
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"
#include "mgm/http/rest-api/response/factories/tape/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/controllers/ControllerFactory.hh"


EOSMGMRESTNAMESPACE_BEGIN

TapeRestHandler::TapeRestHandler(const std::string& entryPointURL): RestHandler(entryPointURL){
  mControllerManager.addController(std::shared_ptr<Controller>(ControllerFactory::getStageControllerV1(mEntryPointURL+"v1/stage/")));
}

common::HttpResponse* TapeRestHandler::handleRequest(common::HttpRequest* request, const common::VirtualIdentity * vid) {
  //URL = /entrypoint/version/resource-name/...
  std::string url = request->GetUrl();
  if(isRestRequest(url)) {
    try {
      std::shared_ptr<Controller> controller = mControllerManager.getController(url);
      return controller->handleRequest(request,vid);
    } catch (const ControllerNotFoundException &ex) {
      eos_static_info(ex.what());
      return mTapeRestApiResponseFactory.createNotFoundError().getHttpResponse();
    } catch (const MethodNotAllowedException &ex) {
      eos_static_info(ex.what());
      return mTapeRestApiResponseFactory.createMethodNotAllowedError(ex.what()).getHttpResponse();
    }
  }
  return nullptr;
}

EOSMGMRESTNAMESPACE_END