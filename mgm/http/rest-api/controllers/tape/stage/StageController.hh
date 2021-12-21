// ----------------------------------------------------------------------
// File: StageController.hh
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
#ifndef EOS_STAGECONTROLLER_HH
#define EOS_STAGECONTROLLER_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/controllers/Controller.hh"
#include "mgm/http/rest-api/action/Action.hh"
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
class StageController : public Controller {
public:
  StageController(const std::string & accessURL);
  virtual common::HttpResponse * handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) override;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_STAGECONTROLLER_HH
