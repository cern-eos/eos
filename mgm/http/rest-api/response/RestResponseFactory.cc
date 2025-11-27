// ----------------------------------------------------------------------
// File: RestResponseFactory.cc
// Author: Consolidated REST response factory
// ----------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 ************************************************************************/

#include "mgm/http/rest-api/response/RestResponseFactory.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/json/tape/TapeJsonifiers.hh"

EOSMGMRESTNAMESPACE_BEGIN

RestApiResponse<ErrorModel>
RestResponseFactory::makeError(const common::HttpResponse::ResponseCodes code,
                               const std::string& title,
                               const std::optional<std::string>& detail) const
{
  std::shared_ptr<ErrorModel> errorModel = std::make_shared<ErrorModel>(title,
      static_cast<uint32_t>(code), detail);
  std::shared_ptr<ErrorModelJsonifier> jsonObject =
    std::make_shared<ErrorModelJsonifier>();
  errorModel->setJsonifier(jsonObject);
  return createResponse(errorModel, code);
}

RestApiResponse<ErrorModel> RestResponseFactory::BadRequest(const std::string& detail) const
{
  return makeError(common::HttpResponse::BAD_REQUEST, "Bad request", detail);
}

RestApiResponse<ErrorModel> RestResponseFactory::BadRequest(const JsonValidationException& ex) const
{
  const auto& validationErrors = ex.getValidationErrors();
  std::string detail;
  if (validationErrors != nullptr && validationErrors->hasAnyError()) {
    auto& error = validationErrors->getErrors()->front();
    detail += error->getFieldName() + " - " + error->getReason();
  } else {
    detail = ex.what();
  }
  return makeError(common::HttpResponse::BAD_REQUEST, "JSON Validation error", detail);
}

RestApiResponse<ErrorModel> RestResponseFactory::NotFound() const
{
  return makeError(common::HttpResponse::NOT_FOUND, "Not found", std::nullopt);
}

RestApiResponse<ErrorModel>
RestResponseFactory::MethodNotAllowed(const std::string& detail) const
{
  return makeError(common::HttpResponse::METHOD_NOT_ALLOWED, "Method not allowed", detail);
}

RestApiResponse<ErrorModel> RestResponseFactory::Forbidden(const std::string& detail) const
{
  return makeError(common::HttpResponse::FORBIDDEN, "Forbidden", detail);
}

RestApiResponse<ErrorModel> RestResponseFactory::NotImplemented() const
{
  return makeError(common::HttpResponse::NOT_IMPLEMENTED, "Not implemented", std::nullopt);
}

RestApiResponse<ErrorModel>
RestResponseFactory::InternalError(const std::string& detail) const
{
  return makeError(common::HttpResponse::INTERNAL_SERVER_ERROR, "Internal server error", detail);
}

EOSMGMRESTNAMESPACE_END


