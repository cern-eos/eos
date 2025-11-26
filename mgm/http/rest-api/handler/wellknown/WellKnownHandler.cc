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
#include "WellKnownHandler.hh"
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/http/rest-api/action/wellknown/tape/GetTapeRestApiWellKnown.hh"
#include "mgm/http/rest-api/controllers/wellknown/factories/WellKnownControllerFactory.hh"
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"
#include "mgm/http/rest-api/json/wellknown/tape/jsonifiers/GetTapeWellKnownModelJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

WellKnownHandler::WellKnownHandler(const std::string& accessURL,
                                   const RestApiManager* restApiManager) : RestHandler(accessURL),
  mRestApiManager(restApiManager)
{
  initializeControllers();
}

common::HttpResponse* WellKnownHandler::handleRequest(common::HttpRequest*
    request, const common::VirtualIdentity* vid)
{
  std::string url = request->GetUrl();

  try {
    Controller* controller = mControllerManager.getController(url);
    return controller->handleRequest(request, vid);
  } catch (const NotFoundException& ex) {
    eos_static_info(ex.what());
    return mWellknownResponseFactory.createError(
             common::HttpResponse::NOT_FOUND).getHttpResponse();
  } catch (const MethodNotAllowedException& ex) {
    eos_static_info(ex.what());
    return mWellknownResponseFactory.createError(
             common::HttpResponse::METHOD_NOT_ALLOWED).getHttpResponse();
  } catch (...) {
    std::string errorMsg = "Unknown exception occured";
    eos_static_err(errorMsg.c_str());
    return mWellknownResponseFactory.createError(
             common::HttpResponse::INTERNAL_SERVER_ERROR).getHttpResponse();
  }
}

void WellKnownHandler::initializeControllers()
{
  std::unique_ptr<Controller> wellKnownController =
    WellKnownControllerFactory::getWellKnownController(mEntryPointURL +
        "wlcg-tape-rest-api");
  std::unique_ptr<RestHandler> restHandler = mRestApiManager->getRestHandler(
        mRestApiManager->getTapeRestApiConfig()->getAccessURL());
  std::unique_ptr<TapeRestHandler> tapeRestHandler(static_cast<TapeRestHandler*>
      (restHandler.release()));
  wellKnownController->addAction(std::make_unique<GetTapeRestApiWellKnown>
                                 (wellKnownController->getAccessURL(), common::HttpHandler::Methods::GET,
                                  std::move(tapeRestHandler),
                                  std::make_shared<GetTapeWellKnownModelJsonifier>()));
  mControllerManager.addController(std::move(wellKnownController));
}

EOSMGMRESTNAMESPACE_END