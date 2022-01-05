// ----------------------------------------------------------------------
// File: CreateStageBulkRequest.cc
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


#include "CreateStageBulkRequest.hh"
#include <memory>
#include "XrdSfs/XrdSfsInterface.hh"
#include "mgm/XrdMgmOfs.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"
#include "mgm/bulk-request/prepare/BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/utils/PrepareArgumentsWrapper.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/http/HttpHandler.hh"
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"
#include "mgm/http/rest-api/response/factories/tape/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "common/SymKeys.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiV1ResponseFactory CreateStageBulkRequest::mResponseFactory;

common::HttpResponse* CreateStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkRequestModel;
  try {
    createStageBulkRequestModel = mInputJsonModelBuilder->buildFromJson(request->GetBody());
  } catch (const InvalidJSONException & ex) {
    return mResponseFactory.createBadRequestError(ex.what()).getHttpResponse();
  } catch (const JsonObjectModelMalformedException & ex2){
    return mResponseFactory.createBadRequestError(ex2.what()).getHttpResponse();
  }
  //Create the prepare arguments
  const FilesContainer & files = createStageBulkRequestModel->getFiles();
  bulk::PrepareArgumentsWrapper pargsWrapper("fake_id",Prep_STAGE,files.getOpaqueInfos(),files.getPaths());
  //Stage and persist the bulk-request created by the prepare manager
  bulk::RealMgmFileSystemInterface mgmFsInterface(gOFS);
  bulk::BulkRequestPrepareManager pm(mgmFsInterface);
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness = createBulkRequestBusiness();
  pm.setBulkRequestBusiness(bulkRequestBusiness);
  XrdOucErrInfo error;
  int prepareRetCode = pm.prepare(*pargsWrapper.getPrepareArguments(),error,vid);
  if(prepareRetCode != SFS_DATA){
    //A problem occured, return the error to the client
    return mResponseFactory.createInternalServerError(error.getErrText()).getHttpResponse();
  }
  //Get the bulk-request
  std::shared_ptr<bulk::BulkRequest> bulkRequest = pm.getBulkRequest();
  const std::string & clientRequest = request->GetBody();
  std::string host;
  try {
    host = request->GetHeaders().at("host");
  } catch(const std::out_of_range &ex){
    return mResponseFactory.createInternalServerError("No host information found in the header of the request").getHttpResponse();
  }
  //Persist the user request in the extended attribute of the directory where the bulk-request is saved
  std::map<std::string, std::string> attributes;
  common::SymKey::Base64Encode(clientRequest.c_str(),clientRequest.size(),attributes["base64jsonrequest"]);
  try {
    bulkRequestBusiness->addOrUpdateAttributes(bulkRequest, attributes);
  } catch (const bulk::PersistencyException &ex) {
    return mResponseFactory.createInternalServerError("Unable to persist the attributes of the bulk-request").getHttpResponse();
  }
  //Generate the bulk-request access URL
  std::string bulkRequestAccessURL = URLBuilder::getInstance()
                                         ->setHttpsProtocol()
                                         ->setHostname(host)
                                         ->setControllerAccessURL(mURLPattern)
                                         ->setRequestId(bulkRequest->getId())->build();
  //Prepare the response and return it
  std::shared_ptr<CreatedStageBulkRequestResponseModel> createdStageBulkRequestModel(new CreatedStageBulkRequestResponseModel(clientRequest,bulkRequestAccessURL));
  return mResponseFactory.createCreatedStageRequestResponse(createdStageBulkRequestModel).getHttpResponse();
}

std::shared_ptr<bulk::BulkRequestBusiness>
CreateStageBulkRequest::createBulkRequestBusiness(){
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  return std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
}

EOSMGMRESTNAMESPACE_END