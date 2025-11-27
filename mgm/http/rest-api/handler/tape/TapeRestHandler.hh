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
#include "mgm/http/rest-api/router/Router.hh"
#include "mgm/http/rest-api/action/Action.hh"
#include "common/VirtualIdentity.hh"
#include "mgm/http/rest-api/response/RestResponseFactory.hh"
#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeWellKnownInfos.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class handles the HTTP requests that are
 * intended for the WLCG TAPE REST API
 */
class TapeRestHandler : public RestHandler
{
public:
  /**
   * Constructor of the TapeRestHandler
   * @param restApiUrl the base URL of the REST API without the instance name
   */
  TapeRestHandler(const TapeRestApiConfig* config);
  /**
   * Handles the user request
   * @param request the user request
   * @param vid the virtual identity of the user
   * @return the HttpResponse to the user request
   */
  common::HttpResponse* handleRequest(common::HttpRequest* request,
                                      const common::VirtualIdentity* vid) override;
  /**
   * Returns true if the request URL coming from the client matches the Tape REST API access URL but also:
   * - the tape REST API is activated
   * - a sitename has been configured in the MGM configuration file
   * - the MGM configuration file contains the tapeenabled flag and it is set to true
   * if the tape REST API is activated and if a sitename has been configured in the MGM configuration file
   * @param requestURL the URL called by the client
   * @param errorMsg a string allowing to indicate why the request will not trigger a tape REST API call
   */
  bool isRestRequest(const std::string& requestURL,
                     std::string& errorMsg) const override;

  /**
   * Returns a builder object allowing to build URLs that are linked to the tape REST API
   */
  std::unique_ptr<URLBuilder> getAccessURLBuilder() const;

  /**
   * Returns some information useful for building the .well-known endpoint of this tape REST API
   */
  const TapeWellKnownInfos* getWellKnownInfos() const;

private:

  enum class ApiVersion {
    V0Dot1,
    V1
  };

  static constexpr ApiVersion DEFAULT_API_VERSION = ApiVersion::V1;

  /**
   * Convert the Tape REST API version to it's string representation
   * @param apiVersion the version of the Tape REST API
   */
  std::string apiVersionToStr(ApiVersion apiVersion);

  /**
   * Initialize a version of the tape REST API
   * @param apiVersion the version of the Tape REST API to initialize
   */
  void initialize(TapeRestHandler::ApiVersion apiVersion);

  /**
   * Initializes the STAGE controller for a specific version
   * @param apiVersion the version to apply to this stage controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @param config the configuration of the tape REST API
   * @return the StageController for a specific version
   */
  void initializeStageRoutes(TapeRestHandler::ApiVersion apiVersion,
                             std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);

  /**
   * Initializes the ARCHIVEINFO controller for a specific version
   * @param apiVersion the version to apply to this ARCHIVEINFO controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @return the ArchiveInfoController for a specific version
   */
  void initializeArchiveinfoRoutes(TapeRestHandler::ApiVersion apiVersion,
                                   std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);
  /**
   * Initializes the RELEASE controller for a specific version
   * @param apiVersion the version to apply to this RELEASE controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @return the ReleaseController for a specific version
   */
  void initializeReleaseRoutes(TapeRestHandler::ApiVersion apiVersion,
                               std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);

  /**
   * Initialize the well-known information
   * that will later be used by the .well-known handler
   */
  void initializeTapeWellKnownInfos();

  /**
   * Adds the tape REST API endpoint to the well-known information
   * that will later be used by the .well-known handler
   * @param version
   */
  void addEndpointToWellKnown(const std::string& version);

  /**
   * Adds the tape REST API endpoint to the well-known information
   * that will later be used by the .well-known handler
   * @param version
   * @param url
   */
  void addEndpointToWellKnown(const std::string& version, const std::string& url);

  /**
   * HttpResponse factory for the tape REST API
   */
  RestResponseFactory mTapeRestApiResponseFactory;
  const TapeRestApiConfig* mTapeRestApiConfig;
  std::unique_ptr<TapeWellKnownInfos> mTapeWellKnownInfos;
  Router mRouter;
  std::vector<std::unique_ptr<Action>> mActions;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTHANDLER_HH
