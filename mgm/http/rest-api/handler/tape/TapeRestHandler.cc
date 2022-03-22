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
#include "mgm/http/rest-api/exception/ForbiddenException.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/controllers/tape/factories/ControllerFactory.hh"
#include "mgm/http/rest-api/action/tape/stage/CreateStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/CancelStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/GetStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/DeleteStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/archiveinfo/GetArchiveInfo.hh"
#include "mgm/http/rest-api/action/tape/release/CreateReleaseBulkRequest.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/http/rest-api/json/tape/model-builders/CreateStageRequestModelBuilder.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/CreatedStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/GetStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/archiveinfo/GetArchiveInfoResponseJsonifier.hh"
#include "mgm/http/rest-api/json/tape/model-builders/PathsModelBuilder.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/IMaster.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestHandler::TapeRestHandler(const TapeRestApiConfig* config): RestHandler(
    config->getAccessURL()), mIsActivated(config->isActivated()),
  mSiteName(config->getSiteName()), mIsTapeEnabled(config->isTapeEnabled())
{
  initializeControllers(config);
}

void TapeRestHandler::initializeControllers(const TapeRestApiConfig* config)
{
  std::shared_ptr<TapeRestApiBusiness> restApiBusiness =
    std::make_shared<TapeRestApiBusiness>();
  std::unique_ptr<Controller> stageController = initializeStageController(
        VERSION_0, restApiBusiness, config);
  mControllerManager.addController(std::move(stageController));
  std::unique_ptr<Controller> fileInfoController =
    initializeArchiveinfoController(VERSION_0, restApiBusiness);
  mControllerManager.addController(std::move(fileInfoController));
  std::unique_ptr<Controller> releaseController = initializeReleaseController(
        VERSION_0, restApiBusiness);
  mControllerManager.addController(std::move(releaseController));
}

std::unique_ptr<Controller> TapeRestHandler::initializeStageController(
  const std::string& apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,
  const TapeRestApiConfig* config)
{
  std::unique_ptr<Controller> stageController(
    ControllerFactory::getStageController(mEntryPointURL + apiVersion + "/stage/"));
  const std::string& controllerAccessURL = stageController->getAccessURL();
  stageController->addAction(std::make_unique<CreateStageBulkRequest>
                             (controllerAccessURL, common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                              std::make_shared<CreateStageRequestModelBuilder>(mSiteName),
                              std::make_shared<CreatedStageBulkRequestJsonifier>()));
  stageController->addAction(std::make_unique<CancelStageBulkRequest>
                             (controllerAccessURL + "/" + URLParametersConstants::ID + "/cancel",
                              common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                              std::make_shared<PathsModelBuilder>()));
  stageController->addAction(std::make_unique<GetStageBulkRequest>
                             (controllerAccessURL + "/" + URLParametersConstants::ID,
                              common::HttpHandler::Methods::GET, tapeRestApiBusiness,
                              std::make_shared<GetStageBulkRequestJsonifier>()));
  stageController->addAction(std::make_unique<DeleteStageBulkRequest>
                             (controllerAccessURL + "/" + URLParametersConstants::ID,
                              common::HttpHandler::Methods::DELETE, tapeRestApiBusiness));
  return stageController;
}

std::unique_ptr<Controller> TapeRestHandler::initializeArchiveinfoController(
  const std::string& apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  std::unique_ptr<Controller> archiveInfoController(
    ControllerFactory::getArchiveInfoController(mEntryPointURL + apiVersion +
        "/archiveinfo/"));
  const std::string& archiveinfoControllerAccessURL =
    archiveInfoController->getAccessURL();
  archiveInfoController->addAction(std::make_unique<GetArchiveInfo>(
                                     archiveinfoControllerAccessURL, common::HttpHandler::Methods::POST,
                                     tapeRestApiBusiness, std::make_shared<PathsModelBuilder>(),
                                     std::make_shared<GetArchiveInfoResponseJsonifier>()));
  return archiveInfoController;
}

std::unique_ptr<Controller> TapeRestHandler::initializeReleaseController(
  const std::string& apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  std::unique_ptr<Controller> releaseController(
    ControllerFactory::getReleaseController(mEntryPointURL + apiVersion +
        "/release/"));
  const std::string& releaseControllerAccessURL =
    releaseController->getAccessURL();
  releaseController->addAction(std::make_unique<CreateReleaseBulkRequest>
                               (releaseControllerAccessURL + URLParametersConstants::ID,
                                common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                                std::make_shared<PathsModelBuilder>()));
  return releaseController;
}

bool TapeRestHandler::isRestRequest(const std::string& requestURL)
{
  bool siteNameEmpty = mSiteName.empty();
  bool isRestRequest = RestHandler::isRestRequest(requestURL);

  if (isRestRequest) {
    if (siteNameEmpty) {
      std::string errorMsg =
        std::string("msg=\"No taperestapi.sitename has been specified, the tape REST API is therefore disabled\"")
        +
        " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsg.c_str());
      return false;
    }

    if (!mIsActivated) {
      std::string errorMsg =
        std::string(
          "msg=\"The tape REST API is not enabled, verify that the \"") +
        rest::TAPE_REST_API_SWITCH_ON_OFF + "\" space configuration is set to \"on\"\""
        +
        " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsg.c_str());
      return false;
    }

    if (!mIsTapeEnabled) {
      std::string errorMsg =
        std::string(
          "msg=\"The MGM tapeenabled flag has not been set or is set to false, the tape REST API is therefore disabled. Verify that the tapeenabled flag is set to true on the MGM configuration file.\"")
        +
        " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsg.c_str());
      return false;
    }
  }

  return mIsActivated && !siteNameEmpty && mIsTapeEnabled && isRestRequest;
}

common::HttpResponse* TapeRestHandler::handleRequest(common::HttpRequest*
    request, const common::VirtualIdentity* vid)
{
  //URL = /entrypoint/version/resource-name/...
  std::string url = request->GetUrl();

  if (gOFS != nullptr && !gOFS->mMaster->IsMaster()) {
    return mTapeRestApiResponseFactory.createInternalServerError("The tape REST API can only be called on a MASTER MGM").getHttpResponse();
  }

  try {
    Controller* controller = mControllerManager.getController(url);
    return controller->handleRequest(request, vid);
  } catch (const ControllerNotFoundException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createNotFoundError().getHttpResponse();
  } catch (const MethodNotAllowedException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createMethodNotAllowedError(
             ex.what()).getHttpResponse();
  } catch (const ForbiddenException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createForbiddenError(
             ex.what()).getHttpResponse();
  } catch (const RestException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createInternalServerError(
             ex.what()).getHttpResponse();
  } catch (...) {
    std::string errorMsg = "Unknown exception occured";
    eos_static_err(errorMsg.c_str());
    return mTapeRestApiResponseFactory.createInternalServerError(
             errorMsg).getHttpResponse();
  }

  return nullptr;
}

EOSMGMRESTNAMESPACE_END