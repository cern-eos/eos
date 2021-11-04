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

#include "common/http/HttpResponse.hh"
#include "common/http/PlainHttpResponse.hh"
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/model/tape/ErrorModel.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include <memory>

EOSMGMRESTNAMESPACE_BEGIN

/**
 * This class allows to create a RestAPI http response
 * from a model object
 * @tparam T the type of the model to create the http response from
 */
template<typename T>
class RestApiResponse {
public:
  /**
   * Constructor with the model
   * @param model the model that will be used to create the http response
   */
  RestApiResponse(const std::shared_ptr<T> model);
  RestApiResponse(const std::shared_ptr<T> model, const common::HttpResponse::ResponseCodes retCode);
  /**
   * Sets the return code of the HttpResponse
   * @param retCode the code to associate to the HttpResponse
   */
  void setRetCode(const common::HttpResponse::ResponseCodes retCode);

  /**
   * Returns the actual HttpResponse created from the model and the return code
   * of this instance
   * The body will be the JSON representation of the model
   */
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
