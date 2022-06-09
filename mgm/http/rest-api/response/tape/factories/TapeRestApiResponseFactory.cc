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

EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createError(
  const common::HttpResponse::ResponseCodes code, const std::string& title,
  const std::string& detail) const
{
  std::shared_ptr<ErrorModel> errorModel = std::make_shared<ErrorModel>(title,
      static_cast<uint32_t>(code), detail);
  std::shared_ptr<ErrorModelJsonifier> jsonObject =
    std::make_shared<ErrorModelJsonifier>();
  errorModel->setJsonifier(jsonObject);
  return createResponse(errorModel, code);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createBadRequestError(
  const std::string& detail) const
{
  return createError(common::HttpResponse::BAD_REQUEST, "Bad request", detail);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createBadRequestError(
  const JsonValidationException& ex) const
{
  //The bad request error will only return to the client the first validation error encountered
  const auto& validationErrors = ex.getValidationErrors();
  std::string detail;

  if (validationErrors != nullptr && validationErrors->hasAnyError()) {
    auto& error = validationErrors->getErrors()->front();
    detail += error->getFieldName() + " - " + error->getReason();
  } else {
    detail = ex.what();
  }

  return createError(common::HttpResponse::BAD_REQUEST, "JSON Validation error",
                     detail);
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createNotFoundError()
const
{
  return createError(common::HttpResponse::NOT_FOUND, "Not found", "");
}

RestApiResponse<ErrorModel>
TapeRestApiResponseFactory::createMethodNotAllowedError(
  const std::string& detail) const
{
  return createError(common::HttpResponse::METHOD_NOT_ALLOWED,
                     "Method not allowed", detail);
}

RestApiResponse<ErrorModel>
TapeRestApiResponseFactory::createInternalServerError(const std::string& detail)
const
{
  return createError(common::HttpResponse::INTERNAL_SERVER_ERROR,
                     "Internal server error", detail);
}

RestApiResponse<ErrorModel>
TapeRestApiResponseFactory::createNotImplementedError() const
{
  return createError(common::HttpResponse::NOT_IMPLEMENTED, "Not implemented",
                     "");
}

RestApiResponse<void> TapeRestApiResponseFactory::createOkEmptyResponse() const
{
  return RestApiResponse<void>();
}

RestApiResponse<ErrorModel> TapeRestApiResponseFactory::createForbiddenError(
  const std::string& detail) const
{
  return createError(common::HttpResponse::FORBIDDEN, "Forbidden", detail);
}


EOSMGMRESTNAMESPACE_END
