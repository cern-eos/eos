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

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createError(const common::HttpResponse::ResponseCodes code, const std::string & title, const std::string& detail){
  std::shared_ptr<ErrorModel> errorModel(new ErrorModel(title,static_cast<uint32_t>(code),detail));
  return RestApiResponse<ErrorModel>(errorModel,code);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createBadRequestError(const std::string & detail) {
  return createError(common::HttpResponse::BAD_REQUEST,"Bad request",detail);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createNotFoundError() {
  return createError(common::HttpResponse::NOT_FOUND,"Not found","");
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createMethodNotAllowedError(const std::string& detail){
  return createError(common::HttpResponse::METHOD_NOT_ALLOWED,"Method not allowed",detail);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createInternalServerError(const std::string& detail){
  return createError(common::HttpResponse::INTERNAL_SERVER_ERROR,"Internal server error",detail);
}

RestApiResponse<CreatedStageBulkRequestResponseModel> TapeRestApiResponseFactory::createStageBulkRequestResponse(const std::shared_ptr<CreatedStageBulkRequestResponseModel> createdStageBulkRequestModel){
  RestApiResponse<CreatedStageBulkRequestResponseModel> response(createdStageBulkRequestModel);
  response.setRetCode(common::HttpResponse::CREATED);
  return response;
}

EOSMGMRESTNAMESPACE_END