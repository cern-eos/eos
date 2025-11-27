// ----------------------------------------------------------------------
// File: RestResponseFactory.hh
// Author: Consolidated REST response factory
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) CERN/Switzerland                                       *
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

#ifndef EOS_REST_RESPONSE_FACTORY_HH
#define EOS_REST_RESPONSE_FACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/response/RestApiResponse.hh"
#include "mgm/http/rest-api/exception/JsonValidationException.hh"
#include "mgm/http/rest-api/model/tape/common/ErrorModel.hh"
#include <optional>

EOSMGMRESTNAMESPACE_BEGIN

class RestResponseFactory
{
public:
  RestResponseFactory() = default;

  template<typename Model>
  RestApiResponse<Model> createResponse(std::shared_ptr<Model> model,
                                        const common::HttpResponse::ResponseCodes code) const
  {
    return RestApiResponse<Model>(model, code);
  }

  template<typename Model>
  RestApiResponse<Model> createResponse(std::shared_ptr<Model> model,
                                        const common::HttpResponse::ResponseCodes code,
                                        const common::HttpResponse::HeaderMap& responseHeader) const
  {
    return RestApiResponse<Model>(model, code, responseHeader);
  }

  template<typename Model>
  RestApiResponse<Model> Ok(std::shared_ptr<Model> model) const
  {
    return createResponse(model, common::HttpResponse::OK);
  }

  RestApiResponse<void> OkEmpty() const { return RestApiResponse<void>(); }

  template<typename Model>
  RestApiResponse<Model> Created(std::shared_ptr<Model> model,
                                 const common::HttpResponse::HeaderMap& hdrs) const
  {
    return createResponse(model, common::HttpResponse::CREATED, hdrs);
  }

  RestApiResponse<ErrorModel> BadRequest(const std::string& detail) const;
  RestApiResponse<ErrorModel> BadRequest(const JsonValidationException& ex) const;
  RestApiResponse<ErrorModel> NotFound() const;
  RestApiResponse<ErrorModel> MethodNotAllowed(const std::string& detail) const;
  RestApiResponse<ErrorModel> Forbidden(const std::string& detail) const;
  RestApiResponse<ErrorModel> NotImplemented() const;
  RestApiResponse<ErrorModel> InternalError(const std::string& detail) const;

private:
  RestApiResponse<ErrorModel> makeError(const common::HttpResponse::ResponseCodes code,
                                        const std::string& title,
                                        const std::optional<std::string>& detail) const;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_REST_RESPONSE_FACTORY_HH


