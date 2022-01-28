// ----------------------------------------------------------------------
// File: StageControllerV1.hh
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
#ifndef EOS_STAGECONTROLLERV1_HH
#define EOS_STAGECONTROLLERV1_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/controllers/Controller.hh"
#include "mgm/http/rest-api/controllers/Action.hh"
#include "mgm/bulk-request/prepare/StageBulkRequest.hh"
#include "mgm/bulk-request/prepare/BulkRequestPrepareManager.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include "mgm/bulk-request/business/BulkRequestBusiness.hh"
#include "mgm/http/rest-api/response/factories/tape/v1/TapeRestApiV1ResponseFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This controller is the version 1 of the stage
 * resource of the tape REST API
 */
class StageControllerV1 : public Controller {
public:
  StageControllerV1(const std::string & accessURL);
  virtual common::HttpResponse * handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) override;

private:

  static TapeRestApiV1ResponseFactory mResponseFactory;

  /**
   * Creates and persist a stage bulk-request
   */
  class CreateStageBulkRequest : public Action {
  public:
    CreateStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method): Action(accessURL,method){}
    /**
     * Creates and persist a stage bulk-request
     * @param request the client's request
     * @param vid the virtual identity of the client
     * @return the HttpResponse to the client's request (JSON body)
     */
    common::HttpResponse * run(common::HttpRequest * request,const common::VirtualIdentity * vid) override;
  };

  /**
   * Cancels the stage bulk-request
   */
  class CancelStageBulkRequest : public Action {
  public:
    CancelStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method):Action(accessURL,method){}

    /**
     * Cancels the stage bulk-request
     * @param request the client's request
     * @param vid the virtual identity of the client
     * @return the HttpResponse to the client's request (JSON body)
     */
    common::HttpResponse * run(common::HttpRequest * request,const common::VirtualIdentity * vid) override;
  };

  /**
   * Returns all the stage bulk-request known by the system (in the persistence)
   */
  class GetStageBulkRequest : public Action {
  public:
    GetStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method): Action(accessURL,method){}

    /**
     * Get all the stage bulk-request known by the system (in the persistence)
     * @param request the client's request
     * @param vid the virtual identity of the client
     * @return the HttpResponse to the client's request (JSON body)
     */
    common::HttpResponse * run(common::HttpRequest * request,const common::VirtualIdentity * vid) override;
  };

  static std::shared_ptr<bulk::BulkRequestBusiness> createBulkRequestBusiness();
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_STAGECONTROLLERV1_HH
