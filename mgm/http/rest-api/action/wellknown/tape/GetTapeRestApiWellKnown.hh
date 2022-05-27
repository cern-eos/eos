// ----------------------------------------------------------------------
// File: GetTapeRestApiWellKnown.hh
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

#ifndef EOS_GETTAPERESTAPIWELLKNOWN_HH
#define EOS_GETTAPERESTAPIWELLKNOWN_HH
#include "common/json/Jsonifier.hh"
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/action/Action.hh"
#include "mgm/http/rest-api/manager/RestApiManager.hh"
#include "mgm/http/rest-api/response/tape/factories/TapeRestApiResponseFactory.hh"
#include "mgm/http/rest-api/model/wellknown/tape/GetTapeWellKnownModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

class GetTapeRestApiWellKnown : public Action
{
public:
  GetTapeRestApiWellKnown(const std::string& accessURLPattern,
                          const common::HttpHandler::Methods method,
                          std::unique_ptr<TapeRestHandler> tapeRestHandler,
                          std::shared_ptr<common::Jsonifier<GetTapeWellKnownModel>>
                          outputJsonModelBuilder);
  /**
   * Returns the discovery endpoint (.well-known) allowing
   * the client to identify the tape REST API.
   * @param request the client request
   * @param vid the client vid
   */
  common::HttpResponse* run(common::HttpRequest* request,
                            const common::VirtualIdentity* vid) override;
private:
  //We use the tape REST API response factory to get the same structure of error message
  TapeRestApiResponseFactory mResponseFactory;
  //A pointer to the Tape REST Handler in order to get its wellknown information
  std::unique_ptr<TapeRestHandler> mTapeRestHandler;
  //The jsonifier
  std::shared_ptr<common::Jsonifier<GetTapeWellKnownModel>>
      mOutputObjectJsonifier;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETTAPERESTAPIWELLKNOWN_HH
