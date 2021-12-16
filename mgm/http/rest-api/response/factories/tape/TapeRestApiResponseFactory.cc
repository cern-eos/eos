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
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonObject.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse TapeRestApiResponseFactory::createError(const common::HttpResponse::ResponseCodes code, const std::string & title, const std::string& detail) const {
  return RestApiResponse(std::make_shared<TapeRestApiJsonObject<ErrorModel>>(title,static_cast<uint32_t>(code),detail),code);
}

RestApiResponse TapeRestApiResponseFactory::createBadRequestError(const std::string & detail) const {
  return createError(common::HttpResponse::BAD_REQUEST,"Bad request",detail);
}

RestApiResponse TapeRestApiResponseFactory::createNotFoundError() const {
  return createError(common::HttpResponse::NOT_FOUND,"Not found","");
}

RestApiResponse TapeRestApiResponseFactory::createMethodNotAllowedError(const std::string& detail) const {
  return createError(common::HttpResponse::METHOD_NOT_ALLOWED,"Method not allowed",detail);
}

RestApiResponse TapeRestApiResponseFactory::createInternalServerError(const std::string& detail) const {
  return createError(common::HttpResponse::INTERNAL_SERVER_ERROR,"Internal server error",detail);
}

RestApiResponse TapeRestApiResponseFactory::createOkEmptyResponse() const {
  return RestApiResponse();
}

EOSMGMRESTNAMESPACE_END