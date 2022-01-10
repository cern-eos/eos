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
#include "mgm/XrdMgmOfs.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/http/HttpHandler.hh"
#include "mgm/http/rest-api/exception/InvalidJSONException.hh"
#include "mgm/http/rest-api/exception/JsonObjectModelMalformedException.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/http/rest-api/exception/tape/TapeRestApiBusinessException.hh"
#include "common/SymKeys.hh"

EOSMGMRESTNAMESPACE_BEGIN

common::HttpResponse* CreateStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkRequestModel;
  try {
    createStageBulkRequestModel = mInputJsonModelBuilder->buildFromJson(request->GetBody());
  } catch (const InvalidJSONException & ex) {
    return mResponseFactory.createBadRequestError(ex.what()).getHttpResponse();
  } catch (const JsonObjectModelMalformedException & ex){
    return mResponseFactory.createBadRequestError(ex.what()).getHttpResponse();
  }
  //Create the prepare arguments
  std::shared_ptr<bulk::BulkRequest> bulkRequest;
  try {
    bulkRequest = mTapeRestApiBusiness->createStageBulkRequest(createStageBulkRequestModel.get(), vid);
  } catch (const TapeRestApiBusinessException &ex){
    return mResponseFactory.createInternalServerError(ex.what()).getHttpResponse();
  }
  //const std::string & clientRequest = request->GetBody();
  std::string host;
  try {
    host = request->GetHeaders().at("host");
  } catch(const std::out_of_range &ex){
    return mResponseFactory.createInternalServerError("No host information found in the header of the request").getHttpResponse();
  }
  //Persist the user request in the extended attribute of the directory where the bulk-request is saved
  /*std::map<std::string, std::string> attributes;
  common::SymKey::Base64Encode(clientRequest.c_str(),clientRequest.size(),attributes["base64jsonrequest"]);
  try {
    bulkRequestBusiness->addOrUpdateAttributes(bulkRequest, attributes);
  } catch (const bulk::PersistencyException &ex) {
    return mResponseFactory.createInternalServerError("Unable to persist the attributes of the bulk-request").getHttpResponse();
  }*/
  //Generate the bulk-request access URL
  std::string bulkRequestAccessURL = URLBuilder::getInstance()
                                         ->setHttpsProtocol()
                                         ->setHostname(host)
                                         ->setControllerAccessURL(mURLPattern)
                                         ->setRequestId(bulkRequest->getId())->build();
  //Prepare the response and return it
  std::shared_ptr<CreatedStageBulkRequestResponseModel> createdStageBulkRequestModel(new CreatedStageBulkRequestResponseModel(/*clientRequest,*/bulkRequestAccessURL));
  createdStageBulkRequestModel->setJsonifier(mOutputObjectJsonifier);
  return mResponseFactory.createResponse(createdStageBulkRequestModel,common::HttpResponse::ResponseCodes::CREATED).getHttpResponse();
}

EOSMGMRESTNAMESPACE_END