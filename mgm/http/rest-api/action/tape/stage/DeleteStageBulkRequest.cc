// ----------------------------------------------------------------------
// File: DeleteStageBulkRequest.cc
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
#include "DeleteStageBulkRequest.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/model/tape/stage/CancelStageBulkRequestModel.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/manager/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiV1ResponseFactory DeleteStageBulkRequest::mResponseFactory;

common::HttpResponse* DeleteStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;
  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  std::string requestId = requestParameters[URLParametersConstants::ID];
  //Get the prepare request from the persistency
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness = createBulkRequestBusiness();
  auto bulkRequest = bulkRequestBusiness->getBulkRequest(requestId,bulk::BulkRequest::Type::PREPARE_STAGE);
  if(bulkRequest == nullptr) {
    return mResponseFactory.createNotFoundError().getHttpResponse();
  }
  //Create the prepare arguments, we will cancel all the files from this bulk-request
  auto filesFromBulkRequest = bulkRequest->getFiles();
  FilesContainer filesToCancel;
  for(auto & fileFromBulkRequest: *filesFromBulkRequest){
    filesToCancel.addFile(fileFromBulkRequest.first);
  }
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId,Prep_CANCEL,filesToCancel.getOpaqueInfos(),filesToCancel.getPaths());
  bulk::BulkRequestPrepareManager pm(std::make_unique<bulk::RealMgmFileSystemInterface>(gOFS));
  XrdOucErrInfo error;
  pm.prepare(*pargsWrapper.getPrepareArguments(),error,vid);
  //Now that the request got cancelled, let's delete it from the persistency
  try {
    bulkRequestBusiness->deleteBulkRequest(std::move(bulkRequest));
  } catch (bulk::PersistencyException &ex) {
    return mResponseFactory.createInternalServerError(ex.what()).getHttpResponse();
  }
  return mResponseFactory.createOkEmptyResponse().getHttpResponse();
}

std::shared_ptr<bulk::BulkRequestBusiness> DeleteStageBulkRequest::createBulkRequestBusiness(){
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  return std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
}

EOSMGMRESTNAMESPACE_END