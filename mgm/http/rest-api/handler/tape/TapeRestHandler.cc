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
#include "mgm/http/rest-api/action/tape/TapeActions.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"
#include "mgm/http/rest-api/json/tape/TapeModelBuilders.hh"
#include "mgm/http/rest-api/response/ErrorHandling.hh"

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
  switch (apiVersion) {
  case ApiVersion::V0Dot1:
    // No routes exposed for v0.1
    break;
  case ApiVersion::V1:
    initializeStageRoutes(apiVersion, restApiBusiness);
    break ;
  default:
    throw std::invalid_argument("Unknown Tape REST API version. Failed to initialize.");
  }
  initializeArchiveinfoRoutes(apiVersion, restApiBusiness);
  initializeReleaseRoutes(apiVersion, restApiBusiness);
}

void TapeRestHandler::initializeStageRoutes(
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  const std::string controllerAccessURL = mEntryPointURL + apiVersionToStr(apiVersion) + "/stage/";
  mActions.emplace_back(std::make_unique<CreateStageBulkRequest>(
                           controllerAccessURL, common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                           std::make_shared<CreateStageRequestModelBuilder>(mTapeRestApiConfig->getSiteName()),
                           std::make_shared<CreatedStageBulkRequestJsonifier>(), this));
  auto* createStage = mActions.back().get();
  mRouter.add(controllerAccessURL, common::HttpHandler::Methods::POST,
              [createStage](auto* req, auto* vid) { return createStage->run(req, vid); });

  mActions.emplace_back(std::make_unique<CancelStageBulkRequest>(
                           controllerAccessURL + "/" + URLPARAM_ID + "/cancel",
                           common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                           std::make_shared<PathsModelBuilder>()));
  auto* cancelStage = mActions.back().get();
  mRouter.add(controllerAccessURL + "/" + URLPARAM_ID + "/cancel",
              common::HttpHandler::Methods::POST,
              [cancelStage](auto* req, auto* vid) { return cancelStage->run(req, vid); });

  mActions.emplace_back(std::make_unique<GetStageBulkRequest>(
                           controllerAccessURL + "/" + URLPARAM_ID,
                           common::HttpHandler::Methods::GET, tapeRestApiBusiness,
                           std::make_shared<GetStageBulkRequestJsonifier>()));
  auto* getStage = mActions.back().get();
  mRouter.add(controllerAccessURL + "/" + URLPARAM_ID,
              common::HttpHandler::Methods::GET,
              [getStage](auto* req, auto* vid) { return getStage->run(req, vid); });

  mActions.emplace_back(std::make_unique<DeleteStageBulkRequest>(
                           controllerAccessURL + "/" + URLPARAM_ID,
                           common::HttpHandler::Methods::DELETE, tapeRestApiBusiness));
  auto* deleteStage = mActions.back().get();
  mRouter.add(controllerAccessURL + "/" + URLPARAM_ID,
              common::HttpHandler::Methods::DELETE,
              [deleteStage](auto* req, auto* vid) { return deleteStage->run(req, vid); });
}

void TapeRestHandler::initializeArchiveinfoRoutes(
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  const std::string accessURL = mEntryPointURL + apiVersionToStr(apiVersion) + "/archiveinfo/";
  mActions.emplace_back(std::make_unique<GetArchiveInfo>(
                           accessURL, common::HttpHandler::Methods::POST,
                           tapeRestApiBusiness, std::make_shared<PathsModelBuilder>(),
                           std::make_shared<GetArchiveInfoResponseJsonifier>()));
  auto* getArchiveInfo = mActions.back().get();
  mRouter.add(accessURL, common::HttpHandler::Methods::POST,
              [getArchiveInfo](auto* req, auto* vid) { return getArchiveInfo->run(req, vid); });
}

void TapeRestHandler::initializeReleaseRoutes(
  TapeRestHandler::ApiVersion apiVersion,
  std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness)
{
  const std::string accessURL = mEntryPointURL + apiVersionToStr(apiVersion) + "/release/";
  mActions.emplace_back(std::make_unique<CreateReleaseBulkRequest>(
                           accessURL + URLPARAM_ID,
                           common::HttpHandler::Methods::POST, tapeRestApiBusiness,
                           std::make_shared<PathsModelBuilder>()));
  auto* createRelease = mActions.back().get();
  mRouter.add(accessURL + URLPARAM_ID, common::HttpHandler::Methods::POST,
              [createRelease](auto* req, auto* vid) { return createRelease->run(req, vid); });
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
    return mTapeRestApiResponseFactory.InternalError("The tape REST API can only be called on a MASTER MGM").getHttpResponse();
  }

  return HandleWithErrors(mTapeRestApiResponseFactory, [&]() {
    return mRouter.dispatch(request, vid);
  });
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
