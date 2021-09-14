// ----------------------------------------------------------------------
// File: RestApiResponse.hh
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

#ifndef EOS_RESTAPIRESPONSE_HH
#define EOS_RESTAPIRESPONSE_HH

#include "mgm/Namespace.hh"
#include "common/http/HttpResponse.hh"
#include "mgm/http/rest-api/model/tape/ErrorModel.hh"
#include "common/http/PlainHttpResponse.hh"
#include <memory>

EOSMGMRESTNAMESPACE_BEGIN

template<typename T>
class RestApiResponse {
public:
  RestApiResponse(const std::shared_ptr<T> model);
  RestApiResponse(const std::shared_ptr<T> model, const common::HttpResponse::ResponseCodes retCode);
  void setRetCode(const common::HttpResponse::ResponseCodes retCode);
  common::HttpResponse * getHttpResponse() const;
private:
  const std::shared_ptr<T> mModel;
  common::HttpResponse::ResponseCodes mRetCode;
};

template<typename T>
RestApiResponse<T>::RestApiResponse(const std::shared_ptr<T> model):mModel(model) {

}

template<typename T>
RestApiResponse<T>::RestApiResponse(const std::shared_ptr<T> model, const common::HttpResponse::ResponseCodes retCode):mModel(model),mRetCode(retCode) {

}

template<typename T>
void RestApiResponse<T>::setRetCode(const common::HttpResponse::ResponseCodes retCode){
  mRetCode = retCode;
}

template<typename T>
common::HttpResponse * RestApiResponse<T>::getHttpResponse() const{
  common::HttpResponse * response = new common::PlainHttpResponse();
  std::stringstream ss;
  mModel->jsonify(ss);
  response->SetBody(ss.str());
  response->SetResponseCode(mRetCode);
  common::HttpResponse::HeaderMap headerMap;
  headerMap["application/type"] = "json";
  response->SetHeaders(headerMap);
  return response;
}

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTAPIRESPONSE_HH
