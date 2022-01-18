// ----------------------------------------------------------------------
// File: GetFileInfo.cc
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

#include "GetFileInfo.hh"
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/exception/tape/TapeRestApiBusinessException.hh"
#include "mgm/http/rest-api/model/tape/fileinfo/GetFileInfoResponseModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

common::HttpResponse* GetFileInfo::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  std::unique_ptr<PathsModel> paths;
  try {
    paths = mInputJsonModelBuilder->buildFromJson(request->GetBody());
  } catch (const JsonValidationException& ex) {
    return mResponseFactory.createBadRequestError(ex).getHttpResponse();
  }
  //Get the information about the files
  std::shared_ptr<bulk::QueryPrepareResponse> queryPrepareResponse;
  try {
    queryPrepareResponse = mTapeRestApiBusiness->getFileInfo(paths.get(), vid);
  } catch(const TapeRestApiBusinessException & ex) {
    return mResponseFactory.createInternalServerError(ex.what()).getHttpResponse();
  }
  //Build the json response and return it to the client
  std::shared_ptr<GetFileInfoResponseModel> response = std::make_shared<GetFileInfoResponseModel>(queryPrepareResponse);
  response->setJsonifier(mOutputObjectJsonifier);
  return mResponseFactory.createResponse(response,common::HttpResponse::ResponseCodes::OK).getHttpResponse();
}

EOSMGMRESTNAMESPACE_END
