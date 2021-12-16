// ----------------------------------------------------------------------
// File: TapeRestApiV1ResponseFactory.cc
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

#include "TapeRestApiV1ResponseFactory.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiV1JsonObject.hh"
EOSMGMRESTNAMESPACE_BEGIN

TapeRestApiV1ResponseFactory::TapeRestApiV1ResponseFactory(): TapeRestApiResponseFactory(){}

RestApiResponse TapeRestApiV1ResponseFactory::createCreatedStageRequestResponse(std::shared_ptr<CreatedStageBulkRequestResponseModel> model) const {
  return RestApiResponse(std::make_shared<TapeRestApiV1JsonObject<CreatedStageBulkRequestResponseModel>>(model),common::HttpResponse::ResponseCodes::CREATED);
}

RestApiResponse TapeRestApiV1ResponseFactory::createGetStageBulkRequestResponse(std::shared_ptr<bulk::QueryPrepareResponse> model) const {
  return RestApiResponse(std::make_shared<TapeRestApiV1JsonObject<bulk::QueryPrepareResponse>>(model),common::HttpResponse::ResponseCodes::OK);
}
EOSMGMRESTNAMESPACE_END