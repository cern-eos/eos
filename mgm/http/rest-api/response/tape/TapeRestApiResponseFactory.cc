// ----------------------------------------------------------------------
// File: TapeRestApiResponseFactory.cc
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

#include "TapeRestApiResponseFactory.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createError400Response(const std::string & detail) {
  common::HttpResponse::ResponseCodes badRequestCode = common::HttpResponse::BAD_REQUEST;
  std::shared_ptr<ErrorModel> error400Model(new ErrorModel("Bad request",static_cast<uint32_t>(badRequestCode),detail));
  RestApiResponse<ErrorModel> error400Response(error400Model,badRequestCode);
  return error400Response;
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createError404Response() {
  common::HttpResponse::ResponseCodes notFoundCode = common::HttpResponse::NOT_FOUND;
  std::shared_ptr<ErrorModel> error404Model(new ErrorModel("Not found",static_cast<uint32_t>(notFoundCode)));
  RestApiResponse<ErrorModel> error404Response(error404Model,notFoundCode);
  return error404Response;
}

EOSMGMRESTNAMESPACE_END