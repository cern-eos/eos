// ----------------------------------------------------------------------
// File: GetStageBulkRequest.cc
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

#include "GetStageBulkRequest.hh"
#include <memory>
#include "XrdSfs/XrdSfsInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/http/HttpHandler.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiV1ResponseFactory GetStageBulkRequest::mResponseFactory;

common::HttpResponse* GetStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;

  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  std::string requestId = requestParameters[URLParametersConstants::ID];

  //Check existency of the request
  auto bulkRequestBusiness = createBulkRequestBusiness();
  try {
    if (!bulkRequestBusiness->exists(requestId,
                                     bulk::BulkRequest::Type::PREPARE_STAGE)) {
      return mResponseFactory.createNotFoundError().getHttpResponse();
    }
  } catch(bulk::PersistencyException & ex){
    return mResponseFactory.createInternalServerError(ex.what()).getHttpResponse();
  }
  //Instanciate prepare manager
  bulk::BulkRequestPrepareManager pm(std::make_unique<bulk::RealMgmFileSystemInterface>(gOFS));
  pm.setBulkRequestBusiness(bulkRequestBusiness);
  XrdOucErrInfo error;
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId,Prep_QUERY);
  XrdSfsPrep * pargs = pargsWrapper.getPrepareArguments();
  auto queryPrepareResult = pm.queryPrepare(*pargs,error,vid);
  if(!queryPrepareResult->hasQueryPrepareFinished()){
    std::ostringstream oss;
    oss << "Unable to get information about the request " << requestId <<". errMsg=\"" << error.getErrText() << "\"";
    return mResponseFactory.createInternalServerError(oss.str()).getHttpResponse();
  }
  auto queryPrepareResponse = queryPrepareResult->getResponse();
  return mResponseFactory.createGetStageBulkRequestResponse(queryPrepareResponse).getHttpResponse();
}

std::shared_ptr<bulk::BulkRequestBusiness>
GetStageBulkRequest::createBulkRequestBusiness(){
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  return std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
}

EOSMGMRESTNAMESPACE_END
