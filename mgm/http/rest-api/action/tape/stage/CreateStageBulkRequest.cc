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
#include "mgm/ofs/XrdMgmOfs.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/http/HttpHandler.hh"
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/Constants.hh"
#include "mgm/http/rest-api/exception/Exceptions.hh"
#include "common/SymKeys.hh"

EOSMGMRESTNAMESPACE_BEGIN

common::HttpResponse* CreateStageBulkRequest::run(common::HttpRequest* request,
    const common::VirtualIdentity* vid)
{
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<CreateStageBulkRequestModel> createStageBulkRequestModel;

  try {
    createStageBulkRequestModel = mInputJsonModelBuilder->buildFromJson(
                                    request->GetBody());
  } catch (const JsonValidationException& ex) {
    return mResponseFactory.BadRequest(ex).getHttpResponse();
  }

  //Create the prepare arguments
  std::shared_ptr<bulk::BulkRequest> bulkRequest;

  try {
    bulkRequest = mTapeRestApiBusiness->createStageBulkRequest(
                    createStageBulkRequestModel.get(), vid);
  } catch (const TapeRestApiBusinessException& ex) {
    return mResponseFactory.InternalError(ex.what()).getHttpResponse();
  }

  //Prepare the response and return it
  std::shared_ptr<CreatedStageBulkRequestResponseModel>
  createdStageBulkRequestModel(new CreatedStageBulkRequestResponseModel(
                                 bulkRequest->getId()));
  createdStageBulkRequestModel->setJsonifier(mOutputObjectJsonifier);
  //Add the location URL in the response of the request
  common::HttpResponse::HeaderMap responseMap;
  responseMap["Location"] = generateAccessURL(bulkRequest->getId());
  return mResponseFactory.createResponse(createdStageBulkRequestModel,
                                         common::HttpResponse::ResponseCodes::CREATED, responseMap).getHttpResponse();
}

const std::string CreateStageBulkRequest::generateAccessURL(
  const std::string& bulkRequestId)
{
  return mTapeRestHandler->getAccessURLBuilder()->add(getAccessURLPattern())->add(
           bulkRequestId)->build();
}

EOSMGMRESTNAMESPACE_END
