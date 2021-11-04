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
#include "mgm/http/HttpHandler.hh"
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"
#include "mgm/http/rest-api/json/tape/JsonCPPTapeModelBuilder.hh"
#include "mgm/http/rest-api/response/tape/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"

EOSMGMRESTNAMESPACE_BEGIN

StageControllerV1::StageControllerV1(const std::string & accessURL):Controller(accessURL){
  //Add actions to URLs and Http verb
  //A POST on the accessURL of this controller will create and persist a new stage bulk-request
  mControllerActionDispatcher.addAction(mAccessURL,eos::common::HttpHandler::Methods::POST,std::bind(&StageControllerV1::createBulkStageRequest,this,std::placeholders::_1,std::placeholders::_2));
}

common::HttpResponse * StageControllerV1::handleRequest(common::HttpRequest * request,const common::VirtualIdentity * vid) {
  return mControllerActionDispatcher.getAction(request)(request,vid);
}

common::HttpResponse * StageControllerV1::createBulkStageRequest(common::HttpRequest* request, const common::VirtualIdentity * vid) const
{
  //Check the content of the request and create a bulk-request with it
  std::shared_ptr<CreateStageBulkRequestModel> createStageBulkRequestModel;
  JsonCPPTapeModelBuilder builder;
  try {
    createStageBulkRequestModel = builder.buildCreateStageBulkRequestModel(request->GetBody());
  } catch (const InvalidJSONException & ex) {
    return TapeRestApiResponseFactory::createBadRequestError(ex.what()).getHttpResponse();
  } catch (const JsonObjectModelMalformedException & ex2){
    return TapeRestApiResponseFactory::createBadRequestError(ex2.what()).getHttpResponse();
  }
  //Create the prepare arguments
  bulk::PrepareArgumentsWrapper pargsWrapper("fake_id",Prep_STAGE,{""},createStageBulkRequestModel->getPaths());
  //Stage and persist the bulk-request created by the prepare manager
  bulk::RealMgmFileSystemInterface mgmFsInterface(gOFS);
  bulk::BulkRequestPrepareManager pm(mgmFsInterface);
  std::unique_ptr<bulk::AbstractDAOFactory> daoFactory(new bulk::ProcDirectoryDAOFactory(gOFS,*gOFS->mProcDirectoryBulkRequestLocations));
  std::shared_ptr<bulk::BulkRequestBusiness> bulkRequestBusiness(new bulk::BulkRequestBusiness(std::move(daoFactory)));
  pm.setBulkRequestBusiness(bulkRequestBusiness);
  XrdOucErrInfo error;
  int prepareRetCode = pm.prepare(*pargsWrapper.getPrepareArguments(),error,vid);
  if(prepareRetCode != SFS_DATA){
    //A problem occured, return the error to the client
    return TapeRestApiResponseFactory::createInternalServerError(error.getErrText()).getHttpResponse();
  }
  //Get the bulk-request 
  std::shared_ptr<bulk::BulkRequest> bulkRequest = pm.getBulkRequest();
  const std::string & clientRequest = request->GetBody();
  std::string host;
  try {
    host = request->GetHeaders().at("host");
  } catch(const std::out_of_range &ex){
    return TapeRestApiResponseFactory::createInternalServerError("No host information found in the header of the request").getHttpResponse();
  }
  //Generate the bulk-request access URL
  std::string bulkRequestAccessURL = URLBuilder::getInstance()
                                         ->setHttpsProtocol()
                                         ->setHostname(host)
                                         ->setControllerAccessURL(mAccessURL)
                                         ->setRequestId(bulkRequest->getId())->build();
  //Prepare the response and return it
  std::shared_ptr<CreatedStageBulkRequestResponseModel> createdStageBulkRequestModel(new CreatedStageBulkRequestResponseModel(clientRequest,bulkRequestAccessURL));
  return TapeRestApiResponseFactory::createStageBulkRequestResponse(createdStageBulkRequestModel).getHttpResponse();
}

EOSMGMRESTNAMESPACE_END