// ----------------------------------------------------------------------
// File: RestApiResponse.cc
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

#include "RestApiResponse.hh"


EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse::RestApiResponse() : mRetCode(common::HttpResponse::ResponseCodes::OK){}

RestApiResponse::RestApiResponse(const std::shared_ptr<common::JsonObject> object, const common::HttpResponse::ResponseCodes retCode) :
    mJsonObject(object),mRetCode(retCode){}

common::HttpResponse * RestApiResponse::getHttpResponse() const{
  common::HttpResponse * response = new common::PlainHttpResponse();
  if(mJsonObject) {
    common::HttpResponse::HeaderMap headerMap;
    headerMap["application/type"] = "json";
    response->SetHeaders(headerMap);
    std::stringstream ss;
    mJsonObject->jsonify(ss);
    response->SetBody(ss.str());
  }
  response->SetResponseCode(mRetCode);
  return response;
}

EOSMGMRESTNAMESPACE_END