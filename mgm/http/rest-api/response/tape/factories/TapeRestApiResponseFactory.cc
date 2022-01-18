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
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/common/ErrorModelJsonifier.hh"
#include "mgm/http/rest-api/json/tape/jsonifiers/common/JsonValidationErrorModelJsonifier.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createError(const common::HttpResponse::ResponseCodes code, const std::string & title, const std::string& detail) const {
  std::shared_ptr<ErrorModel> errorModel = std::make_shared<ErrorModel>(title,static_cast<uint32_t>(code),detail);
  std::shared_ptr<ErrorModelJsonifier> jsonObject = std::make_shared<ErrorModelJsonifier>();
  errorModel->setJsonifier(jsonObject);
  return createResponse(errorModel,code);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createBadRequestError(const std::string & detail) const {
  return createError(common::HttpResponse::BAD_REQUEST,"Bad request",detail);
}

RestApiResponse<JsonValidationErrorModel> TapeRestApiResponseFactory::createBadRequestError(const JsonValidationException& ex) const {
  std::shared_ptr<JsonValidationErrorModel> errorModel = std::make_shared<JsonValidationErrorModel>(ex.what());
  errorModel->setValidationErrors(ex.getValidationErrors());
  std::shared_ptr<JsonValidationErrorModelJsonifier> jsonifier = std::make_shared<JsonValidationErrorModelJsonifier>();
  errorModel->setJsonifier(jsonifier);
  return createResponse(errorModel,common::HttpResponse::BAD_REQUEST);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createNotFoundError() const {
  return createError(common::HttpResponse::NOT_FOUND,"Not found","");
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createMethodNotAllowedError(const std::string& detail) const {
  return createError(common::HttpResponse::METHOD_NOT_ALLOWED,"Method not allowed",detail);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createInternalServerError(const std::string& detail) const {
  return createError(common::HttpResponse::INTERNAL_SERVER_ERROR,"Internal server error",detail);
}

RestApiResponse<void> TapeRestApiResponseFactory::createOkEmptyResponse() const {
  return RestApiResponse<void>();
}


EOSMGMRESTNAMESPACE_END