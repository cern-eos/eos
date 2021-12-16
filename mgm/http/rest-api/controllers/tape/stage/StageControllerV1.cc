// ----------------------------------------------------------------------
// File: StageControllerV1.cc
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

#include "StageControllerV1.hh"
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
#include "mgm/http/rest-api/json/tape/JsonCPPTapeModelBuilder.hh"
#include "mgm/http/rest-api/response/factories/tape/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "common/SymKeys.hh"

EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiV1ResponseFactory StageControllerV1::mResponseFactory;

StageControllerV1::StageControllerV1(const std::string & accessURL):Controller(accessURL){
  //Add actions to URLs and Http verb
  //A POST on the accessURL of this controller will create and persist a new stage bulk-request
  mControllerActionDispatcher.addAction(std::make_unique<CreateStageBulkRequest>(mAccessURL,common::HttpHandler::Methods::POST));
  mControllerActionDispatcher.addAction(std::make_unique<CancelStageBulkRequest>(mAccessURL + "/" + URLParametersConstants::ID + "/cancel",common::HttpHandler::Methods::POST));
  mControllerActionDispatcher.addAction(std::make_unique<GetStageBulkRequest>(mAccessURL + URLParametersConstants::ID,common::HttpHandler::Methods::GET));
}

common::HttpResponse * StageControllerV1::handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) {
  return mControllerActionDispatcher.getAction(request)->run(request,vid);
}

common::HttpResponse* StageControllerV1::CreateStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkRequestModel;
  JsonCPPTapeModelBuilder builder;
  try {
    createStageBulkRequestModel = builder.buildCreateStageBulkRequestModel(request->GetBody());
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

common::HttpResponse* StageControllerV1::CancelStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<CancelStageBulkRequestModel> cancelStageBulkRequestModel;
  JsonCPPTapeModelBuilder builder;
  try {
    cancelStageBulkRequestModel = builder.buildCancelStageBulkRequestModel(request->GetBody());
  } catch (const InvalidJSONException & ex) {
    return mResponseFactory.createBadRequestError(ex.what()).getHttpResponse();
  } catch (const JsonObjectModelMalformedException & ex2){
    return mResponseFactory.createBadRequestError(ex2.what()).getHttpResponse();
  }
  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  const std::string & requestId = requestParameters[URLParametersConstants::ID];
  //Get the prepare request from the persistency
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness = createBulkRequestBusiness();
  auto bulkRequest = bulkRequestBusiness->getBulkRequest(requestId,bulk::BulkRequest::Type::PREPARE_STAGE);
  if(bulkRequest == nullptr) {
    return mResponseFactory.createNotFoundError().getHttpResponse();
  }
  //Create the prepare arguments, we will only cancel the files that were given by the user
  const FilesContainer & filesFromClient = cancelStageBulkRequestModel->getFiles();
  auto filesFromBulkRequest = bulkRequest->getFiles();
  FilesContainer filesToCancel;
  for(auto & fileFromClient: filesFromClient.getPaths()){
    if(filesFromBulkRequest->find(fileFromClient) != filesFromBulkRequest->end()){
      filesToCancel.addFile(fileFromClient);
    } else {
      std::ostringstream oss;
      oss << "The file " << fileFromClient << " does not belong to the STAGE request " << bulkRequest->getId() << ". No modification has been made to this request.";
      return mResponseFactory.createBadRequestError(oss.str()).getHttpResponse();
    }
  }

  //Do the cancellation
  bulk::PrepareArgumentsWrapper pargsWrapper(requestId,Prep_CANCEL,filesToCancel.getOpaqueInfos(),filesToCancel.getPaths());
  bulk::RealMgmFileSystemInterface mgmFsInterface(gOFS);
  bulk::BulkRequestPrepareManager pm(mgmFsInterface);
  XrdOucErrInfo error;
  pm.prepare(*pargsWrapper.getPrepareArguments(),error,vid);
  return mResponseFactory.createOkEmptyResponse().getHttpResponse();
}

common::HttpResponse* StageControllerV1::GetStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;

  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  std::string requestId = requestParameters[URLParametersConstants::ID];

  //Instanciate prepare manager
  auto bulkRequestBusiness = createBulkRequestBusiness();
  bulk::RealMgmFileSystemInterface mgmFsInterface(gOFS);
  bulk::BulkRequestPrepareManager pm(mgmFsInterface);
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

std::shared_ptr<bulk::BulkRequestBusiness> StageControllerV1::createBulkRequestBusiness(){
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestTapeRestApiLocations));
  return std::make_shared<bulk::BulkRequestBusiness>(std::move(daoFactory));
}

EOSMGMRESTNAMESPACE_END