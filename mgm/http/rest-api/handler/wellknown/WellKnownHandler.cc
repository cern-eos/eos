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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/http/rest-api/handler/tape/TapeRestHandler.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"
#include "mgm/http/rest-api/response/RestResponseFactory.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/response/ErrorHandling.hh"

EOSMGMRESTNAMESPACE_BEGIN

WellKnownHandler::WellKnownHandler(const std::string& accessURL,
                                   const RestApiManager* restApiManager) : RestHandler(accessURL),
  mRestApiManager(restApiManager)
{
  initializeRoutes();
}

common::HttpResponse* WellKnownHandler::handleRequest(common::HttpRequest*
    request, const common::VirtualIdentity* vid)
{
  std::string url = request->GetUrl();

  try {
    return mRouter.dispatch(request, vid);
  } catch (const NotFoundException& ex) {
    eos_static_info(ex.what());
    return mWellknownResponseFactory.NotFound().getHttpResponse();
  } catch (const MethodNotAllowedException& ex) {
    eos_static_info(ex.what());
    return mWellknownResponseFactory.MethodNotAllowed(ex.what()).getHttpResponse();
  } catch (...) {
    std::string errorMsg = "Unknown exception occured";
    eos_static_err(errorMsg.c_str());
    return mWellknownResponseFactory.InternalError(errorMsg).getHttpResponse();
  }
}

void WellKnownHandler::initializeRoutes()
{
  const std::string accessURL = mEntryPointURL + "wlcg-tape-rest-api";
  mRouter.add(accessURL, common::HttpHandler::Methods::GET,
              [this](auto* request, auto* vid) -> common::HttpResponse* {
                RestResponseFactory respFactory;
                std::unique_ptr<RestHandler> restHandler = mRestApiManager->getRestHandler(
                      mRestApiManager->getTapeRestApiConfig()->getAccessURL());
                std::unique_ptr<TapeRestHandler> tapeRestHandler(static_cast<TapeRestHandler*>
                    (restHandler.release()));
                const TapeWellKnownInfos* tapeWellKnownInfos = tapeRestHandler->getWellKnownInfos();
                std::string errorMsg;
                if (!tapeRestHandler->isRestRequest(tapeRestHandler->getEntryPointURL(), errorMsg)) {
                  return respFactory.InternalError(errorMsg).getHttpResponse();
                }
                std::shared_ptr<GetTapeWellKnownModel> model =
                  std::make_shared<GetTapeWellKnownModel>(tapeWellKnownInfos);
                model->setJsonifier(std::make_shared<GetTapeWellKnownModelJsonifier>());
                return respFactory.Ok(model).getHttpResponse();
              });
}

EOSMGMRESTNAMESPACE_END