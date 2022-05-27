// ----------------------------------------------------------------------
// File: GetTapeRestApiWellKnown.cc
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

#include "GetTapeRestApiWellKnown.hh"

EOSMGMRESTNAMESPACE_BEGIN

GetTapeRestApiWellKnown::GetTapeRestApiWellKnown(const std::string&
    accessURLPattern, const common::HttpHandler::Methods method,
    std::unique_ptr<TapeRestHandler> tapeRestHandler,
    std::shared_ptr<common::Jsonifier<GetTapeWellKnownModel>>
    outputJsonModelBuilder) :
  Action(accessURLPattern, method),
  mTapeRestHandler(std::move(tapeRestHandler)),
  mOutputObjectJsonifier(outputJsonModelBuilder) {}

common::HttpResponse* GetTapeRestApiWellKnown::run(common::HttpRequest* request,
    const common::VirtualIdentity* vid)
{
  const mgm::rest::TapeWellKnownInfos* tapeWellKnownInfos =
    mTapeRestHandler->getWellKnownInfos();
  std::string errorMsg;

  //If the Tape REST API is deactivated or misconfigured, an error message will be given to the user
  //indicating what is wrong
  if (!mTapeRestHandler->isRestRequest(mTapeRestHandler->getEntryPointURL(),
                                       errorMsg)) {
    return mResponseFactory.createInternalServerError(errorMsg).getHttpResponse();
  }

  std::shared_ptr<GetTapeWellKnownModel> model =
    std::make_shared<GetTapeWellKnownModel>(tapeWellKnownInfos);
  model->setJsonifier(mOutputObjectJsonifier);
  return mResponseFactory.createResponse(model,
                                         common::HttpResponse::OK).getHttpResponse();
}

EOSMGMRESTNAMESPACE_END