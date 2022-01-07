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
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/controllers/tape/URLParametersConstants.hh"
#include "mgm/http/rest-api/exception/ObjectNotFoundException.hh"
#include "mgm/http/rest-api/exception/tape/TapeRestApiBusinessException.hh"

EOSMGMRESTNAMESPACE_BEGIN

common::HttpResponse* DeleteStageBulkRequest::run(common::HttpRequest* request, const common::VirtualIdentity* vid) {
  URLParser parser(request->GetUrl());
  std::map<std::string,std::string> requestParameters;
  //Get the id of the request from the URL
  parser.matchesAndExtractParameters(this->mURLPattern,requestParameters);
  std::string requestId = requestParameters[URLParametersConstants::ID];
  try {
    mTapeRestApiBusiness->deleteStageBulkRequest(requestId, vid);
  } catch (const ObjectNotFoundException & ex) {
    return mResponseFactory.createNotFoundError().getHttpResponse();
  } catch (const TapeRestApiBusinessException & ex) {
    return mResponseFactory.createInternalServerError(ex.what()).getHttpResponse();
  }
  return mResponseFactory.createOkEmptyResponse().getHttpResponse();
}

EOSMGMRESTNAMESPACE_END