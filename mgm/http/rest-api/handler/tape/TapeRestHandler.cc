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
#include "mgm/imaster/IMaster.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/http/rest-api/action/tape/archiveinfo/GetArchiveInfo.hh"
#include "mgm/http/rest-api/action/tape/release/CreateReleaseBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/CancelStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/CreateStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/DeleteStageBulkRequest.hh"
#include "mgm/http/rest-api/action/tape/stage/GetStageBulkRequest.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/http/rest-api/controllers/tape/factories/TapeControllerFactory.hh"
#include "mgm/http/rest-api/exception/ControllerNotFoundException.hh"
#include "mgm/http/rest-api/exception/ForbiddenException.hh"
#include "mgm/http/rest-api/exception/MethodNotAllowedException.hh"
#include "mgm/http/rest-api/exception/NotImplementedException.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/archiveinfo/GetArchiveInfoResponseJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/CreatedStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/stage/GetStageBulkRequestJsonifier.hh"
#include "mgm/http/rest-api/json/tape/model-builders/CreateStageRequestModelBuilder.hh"
#include "mgm/http/rest-api/json/tape/model-builders/PathsModelBuilder.hh"

EOSMGMRESTNAMESPACE_BEGIN

std::string TapeRestHandler::apiVersionToStr(TapeRestHandler::ApiVersion apiVersion) {
  switch(apiVersion) {
  case ApiVersion::V0Dot1:
    return "v0.1";
  case ApiVersion::V1:
    return "v1";
  default:
    throw std::invalid_argument("Unknown Tape REST API version.");
  }
}

TapeRestHandler::TapeRestHandler(const TapeRestApiConfig* config): RestHandler(
    config->getAccessURL()), mTapeRestApiConfig(config)
{
  initializeTapeWellKnownInfos();
  initialize(DEFAULT_API_VERSION);
  auto endpointToUrlMap = config->getEndpointToUriMapping();
  for (auto & [version, url]: endpointToUrlMap) {
    addEndpointToWellKnown(version, url);
  }
  // If there was no .well-known endpoint provided for the version DEFAULT_API_VERSION
  // construct it with the default setup
  if (!endpointToUrlMap.count(apiVersionToStr(DEFAULT_API_VERSION))) {
    addEndpointToWellKnown(apiVersionToStr(DEFAULT_API_VERSION));
  }
}

void TapeRestHandler::initialize(TapeRestHandler::ApiVersion apiVersion) {
  std::shared_ptr<TapeRestApiBusiness> restApiBusiness =
      std::make_shared<TapeRestApiBusiness>();
  std::unique_ptr<Controller> stageController;
  switch (apiVersion) {
  case ApiVersion::V0Dot1:
    stageController = TapeControllerFactory::getNotImplementedController(mEntryPointURL + apiVersionToStr(apiVersion) +"/stage/");
    break;
  case ApiVersion::V1:
    stageController = initializeStageController(apiVersion, restApiBusiness);
    break ;
  default:
    throw std::invalid_argument("Unknown Tape REST API version. Failed to initialize.");
  }
  mControllerManager.addController(std::move(stageController));
  std::unique_ptr<Controller> fileInfoController = initializeArchiveinfoController(apiVersion, restApiBusiness);
  mControllerManager.addController(std::move(fileInfoController));
  std::unique_ptr<Controller> releaseController = initializeReleaseController(apiVersion, restApiBusiness);
  mControllerManager.addController(std::move(releaseController));
}

std::unique_ptr<Controller> TapeRestHandler::initializeStageController(
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  std::unique_ptr<Controller> stageController(
    TapeControllerFactory::getStageController(mEntryPointURL + apiVersionToStr(apiVersion) +
        "/stage/", mTapeRestApiConfig));
  const std::string& controllerAccessURL = stageController->getAccessURL();
  stageController->addAction(std::make_unique<CreateStageBulkRequest>
                             (controllerAccessURL, common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                              std::make_shared<CreateStageRequestModelBuilder>
                              (mTapeRestApiConfig->getSiteName()),
                              std::make_shared<CreatedStageBulkRequestJsonifier>(), this));
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
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  std::unique_ptr<Controller> archiveInfoController(
    TapeControllerFactory::getArchiveInfoController(mEntryPointURL + apiVersionToStr(apiVersion) +
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
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  std::unique_ptr<Controller> releaseController(
    TapeControllerFactory::getReleaseController(mEntryPointURL + apiVersionToStr(apiVersion) +
        "/release/"));
  const std::string& releaseControllerAccessURL =
    releaseController->getAccessURL();
  releaseController->addAction(std::make_unique<CreateReleaseBulkRequest>
                               (releaseControllerAccessURL + URLParametersConstants::ID,
                                common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                                std::make_shared<PathsModelBuilder>()));
  return releaseController;
}

void TapeRestHandler::initializeTapeWellKnownInfos()
{
  mTapeWellKnownInfos = std::make_unique<TapeWellKnownInfos>
                        (mTapeRestApiConfig->getSiteName());
}

void TapeRestHandler::addEndpointToWellKnown(const std::string& version)
{
  auto accessURLBuilder = getAccessURLBuilder();
  accessURLBuilder->add(mEntryPointURL);
  accessURLBuilder->add(version);
  mTapeWellKnownInfos->addEndpoint(accessURLBuilder->build(), version);
}

void TapeRestHandler::addEndpointToWellKnown(const std::string& version, const std::string& url)
{
  mTapeWellKnownInfos->addEndpoint(url, version);
}

bool TapeRestHandler::isRestRequest(const std::string& requestURL,
                                    std::string& errorMsg) const
{
  bool isRestRequest = RestHandler::isRestRequest(requestURL, errorMsg);

  if (isRestRequest) {
    if (mTapeRestApiConfig->getSiteName().empty()) {
      errorMsg =
        "No taperestapi.sitename has been specified, the tape REST API is therefore disabled";
      std::string errorMsgLog =
        std::string("msg=\"") + errorMsg + "\"" + " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsgLog.c_str());
      return false;
    }

    if (mTapeRestApiConfig->getHostAlias().empty()) {
      errorMsg =
        "No mgmofs.alias has been specified, the tape REST API is therefore disabled";
      std::string errorMsgLog =
        std::string("msg=\"") + errorMsg + "\"" + " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsgLog.c_str());
      return false;
    }

    if (!mTapeRestApiConfig->isActivated()) {
      errorMsg = std::string("The tape REST API is not enabled, verify that the \"") +
                 rest::TAPE_REST_API_SWITCH_ON_OFF + "\" space configuration is set to \"on\"";
      std::string errorMsgLog = std::string("msg=\"") + errorMsg + "\"" +
                                " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsgLog.c_str());
      return false;
    }

    if (!mTapeRestApiConfig->isTapeEnabled()) {
      errorMsg =
        "The MGM tapeenabled flag has not been set or is set to false, the tape REST API is therefore disabled. Verify that the tapeenabled flag is set to true on the MGM configuration file.";
      std::string errorMsgLog =
        std::string("msg=\"") + errorMsg + "\"" + " requestURL=\"" + requestURL + "\"";
      eos_static_warning(errorMsgLog.c_str());
      return false;
    }
  }

  return isRestRequest;
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
  } catch (const NotFoundException& ex) {
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
  } catch (const NotImplementedException& ex) {
    eos_static_info(ex.what());
    return mTapeRestApiResponseFactory.createNotImplementedError().getHttpResponse();
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
}

std::unique_ptr<URLBuilder> TapeRestHandler::getAccessURLBuilder() const
{
  std::unique_ptr<URLBuilder> ret;
  auto builder = URLBuilder::getInstance();
  builder->setHttpsProtocol()->setHostname(
    mTapeRestApiConfig->getHostAlias())->setPort(
      mTapeRestApiConfig->getXrdHttpPort());
  ret.reset(static_cast<URLBuilder*>(builder.release()));
  return ret;
}

const TapeWellKnownInfos* TapeRestHandler::getWellKnownInfos() const
{
  return mTapeWellKnownInfos.get();
}

EOSMGMRESTNAMESPACE_END
