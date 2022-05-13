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
#include "mgm/http/rest-api/config/tape/TapeRestApiConfig.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/wellknown/tape/TapeWellKnownInfos.hh"

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
  TapeRestHandler(const TapeRestApiConfig * config);
  /**
   * Handles the user request
   * @param request the user request
   * @param vid the virtual identity of the user
   * @return the HttpResponse to the user request
   */
  common::HttpResponse * handleRequest(common::HttpRequest * request, const common::VirtualIdentity * vid) override;
  /**
   * Returns true if the request URL coming from the client matches the Tape REST API access URL but also:
   * - the tape REST API is activated
   * - a sitename has been configured in the MGM configuration file
   * - the MGM configuration file contains the tapeenabled flag and it is set to true
   * if the tape REST API is activated and if a sitename has been configured in the MGM configuration file
   * @param requestURL the URL called by the client
   * @param errorMsg a string allowing to indicate why the request will not trigger a tape REST API call
   */
  bool isRestRequest(const std::string & requestURL, std::string & errorMsg) const override;

  /**
   * Returns a builder object allowing to build URLs that are linked to the tape REST API
   */
  std::unique_ptr<URLBuilder> getAccessURLBuilder() const;

  /**
   * Returns some information useful for building the .well-known endpoint of this tape REST API
   */
  const TapeWellKnownInfos * getWellKnownInfos() const;
private:
  /**
   * Initialize the controllers of the tape REST API
   * @param config the configuration object that contains the tape REST API configuration parameters
   */
  void initializeControllers();
  /**
   * Initializes the STAGE controller for a specific version
   * @param apiVersion the version to apply to this stage controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @param config the configuration of the tape REST API
   * @return the StageController for a specific version
   */
  std::unique_ptr<Controller> initializeStageController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);
  /**
   * Initializes the ARCHIVEINFO controller for a specific version
   * @param apiVersion the version to apply to this ARCHIVEINFO controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @return the ArchiveInfoController for a specific version
   */
  std::unique_ptr<Controller> initializeArchiveinfoController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);
  /**
   * Initializes the RELEASE controller for a specific version
   * @param apiVersion the version to apply to this RELEASE controller
   * @param tapeRestApiBusiness the business layer of the tape REST API
   * @return the ReleaseController for a specific version
   */
  std::unique_ptr<Controller> initializeReleaseController(const std::string & apiVersion, std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness);

  /**
   * Edit the content of this method to add extra information to the .well-known endpoint
   */
  std::unique_ptr<TapeWellKnownInfos> initializeTapeWellKnownInfos();

  /**
   * HttpResponse factory for the tape REST API
   */
  TapeRestApiResponseFactory mTapeRestApiResponseFactory;
  inline static const std::string VERSION_0 = "v0";
  const TapeRestApiConfig * mTapeRestApiConfig;
  std::unique_ptr<TapeWellKnownInfos> mTapeWellKnownInfos;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTHANDLER_HH
