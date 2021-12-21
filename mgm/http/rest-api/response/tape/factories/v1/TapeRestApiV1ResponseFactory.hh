// ----------------------------------------------------------------------
// File: TapeRestApiV1ResponseFactory.hh
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

#ifndef EOS_TAPERESTAPIV1RESPONSEFACTORY_HH
#define EOS_TAPERESTAPIV1RESPONSEFACTORY_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"
#include "mgm/bulk-request/response/QueryPrepareResponse.hh"
#include <memory>

EOSMGMRESTNAMESPACE_BEGIN

class TapeRestApiV1ResponseFactory : public TapeRestApiResponseFactory {
public:
  TapeRestApiV1ResponseFactory();
  RestApiResponse createCreatedStageRequestResponse(std::shared_ptr<CreatedStageBulkRequestResponseModel> model) const;
  RestApiResponse createGetStageBulkRequestResponse(std::shared_ptr<bulk::QueryPrepareResponse> getStageBulkRequestResponseModel) const;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_TAPERESTAPIV1RESPONSEFACTORY_HH
