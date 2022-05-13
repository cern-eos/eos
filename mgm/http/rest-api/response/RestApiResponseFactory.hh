// ----------------------------------------------------------------------
// File: TapeRestApiResponseFactory.hh
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

#ifndef EOS_RESTAPIRESPONSEFACTORY_HH
#define EOS_RESTAPIRESPONSEFACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/response/RestApiResponse.hh"

EOSMGMRESTNAMESPACE_BEGIN

class RestApiResponseFactory {
public:
  RestApiResponseFactory() = default;
  template<typename Model>
  RestApiResponse<Model> createResponse(std::shared_ptr<Model> model, const common::HttpResponse::ResponseCodes code) const {
    return RestApiResponse(model,code);
  }

  template<typename Model>
  RestApiResponse<Model> createResponse(std::shared_ptr<Model> model, const common::HttpResponse::ResponseCodes code, const common::HttpResponse::HeaderMap & responseHeader) const {
    return RestApiResponse(model,code,responseHeader);
  }
  ~RestApiResponseFactory() = default;
private:
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_RESTAPIRESPONSEFACTORY_HH
