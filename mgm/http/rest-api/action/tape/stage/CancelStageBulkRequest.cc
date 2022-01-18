// ----------------------------------------------------------------------
// File: CancelStageBulkRequest.cc
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

#include "CancelStageBulkRequest.hh"
#include "mgm/http/rest-api/utils/URLParser.hh"
#include "mgm/http/rest-api/utils/URLBuilder.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/exception/ObjectNotFoundException.hh"
#include "mgm/http/rest-api/exception/tape/FileDoesNotBelongToBulkRequestException.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/bulk-request/interface/RealMgmFileSystemInterface.hh"

EOSMGMRESTNAMESPACE_BEGIN

common::HttpResponse* CancelStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;
  //Check the content of the request and create a bulk-request with it
  std::unique_ptr<PathsModel> paths;
  try {
    paths = mInputJsonModelBuilder->buildFromJson(request->GetBody());
  } catch (const JsonValidationException & ex) {
    return mResponseFactory.createBadRequestError(ex).getHttpResponse();
  }
  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  const std::string & requestId = requestParameters[URLParametersConstants::ID];
  try {
    mTapeRestApiBusiness->cancelStageBulkRequest(requestId, paths.get(), vid);
  } catch (const ObjectNotFoundException &ex){
    return mResponseFactory.createNotFoundError().getHttpResponse();
  } catch(const FileDoesNotBelongToBulkRequestException&ex) {
    return mResponseFactory.createBadRequestError(ex.what()).getHttpResponse();
  }
  return mResponseFactory.createOkEmptyResponse().getHttpResponse();
}


EOSMGMRESTNAMESPACE_END