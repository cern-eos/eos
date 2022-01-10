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
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/controllers/tape/factories/ControllerFactory.hh"
#include "mgm/http/rest-api/action/tape/stage/CreateStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/CancelStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/GetStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/DeleteStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/fileinfo/GetFileInfo.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/http/rest-api/json/tape/model-builders/CreateStageRequestModelBuilder.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/CreatedStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/GetStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/fileinfo/GetFileInfoResponseJsonifier.hh"
#include "mgm/http/rest-api/json/tape/model-builders/PathsModelBuilder.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestHandler::TapeRestHandler(const std::string& entryPointURL): RestHandler(entryPointURL) {
  initializeControllers();
}

void TapeRestHandler::initializeControllers() {
  std::shared_ptr<TapeRestApiBusiness> restApiBusiness = std::make_shared<TapeRestApiBusiness>();
  std::unique_ptr<Controller> stageController = initializeStageController(VERSION_0,restApiBusiness);
  mControllerManager.addController(std::move(stageController));

  std::unique_ptr<Controller> fileInfoController = initializeFileInfoController(VERSION_0,restApiBusiness);
  mControllerManager.addController(std::move(fileInfoController));
}

std::unique_ptr<Controller> TapeRestHandler::initializeStageController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness) {
  std::unique_ptr<Controller> stageController(ControllerFactory::getStageController(mEntryPointURL + apiVersion + "/stage/"));
  const std::string & controllerAccessURL = stageController->getAccessURL();
  stageController->addAction(std::make_unique<CreateStageBulkRequest>(controllerAccessURL,common::HttpHandler::Methods::POST,tapeRestApiBusiness,std::make_shared<CreateStageRequestModelBuilder>(),std::make_shared<CreatedStageBulkRequestJsonifier>()));
  stageController->addAction(std::make_unique<CancelStageBulkRequest>(controllerAccessURL + "/" + URLParametersConstants::ID + "/cancel",common::HttpHandler::Methods::POST,tapeRestApiBusiness,std::make_shared<PathsModelBuilder>()));
  stageController->addAction(std::make_unique<GetStageBulkRequest>(controllerAccessURL + "/" + URLParametersConstants::ID,common::HttpHandler::Methods::GET,tapeRestApiBusiness,std::make_shared<GetStageBulkRequestJsonifier>()));
  stageController->addAction(std::make_unique<DeleteStageBulkRequest>(controllerAccessURL + "/" + URLParametersConstants::ID, common::HttpHandler::Methods::DELETE,tapeRestApiBusiness));
  return stageController;
}

std::unique_ptr<Controller> TapeRestHandler::initializeFileInfoController(const std::string& apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness) {
  std::unique_ptr<Controller> fileInfoController(ControllerFactory::getFileinfoController(mEntryPointURL + apiVersion + "/fileinfo/"));
  const std::string & fileinfoControllerAccessURL = fileInfoController->getAccessURL();
  fileInfoController->addAction(std::make_unique<GetFileInfo>(fileinfoControllerAccessURL,common::HttpHandler::Methods::POST,tapeRestApiBusiness,std::make_shared<PathsModelBuilder>(),std::make_shared<GetFileInfoResponseJsonifier>()));
  return fileInfoController;
}

common::HttpResponse* TapeRestHandler::handleRequest(common::HttpRequest* request, const common::VirtualIdentity * vid) {
  //URL = /entrypoint/version/resource-name/...
  std::string url = request->GetUrl();
  if(isRestRequest(url)) {
    try {
      Controller * controller = mControllerManager.getController(url);
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