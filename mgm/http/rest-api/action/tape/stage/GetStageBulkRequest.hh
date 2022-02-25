// ----------------------------------------------------------------------
// File: GetStageBulkRequest.hh
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
#ifndef EOS_GETSTAGEBULKREQUEST_HH
#define EOS_GETSTAGEBULKREQUEST_HH

#include "mgm/http/rest-api/action/tape/TapeAction.hh"
#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/model/tape/stage/GetStageBulkRequestResponseModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

class GetStageBulkRequest : public TapeAction {
public:
  GetStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method,std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,std::shared_ptr<TapeRestApiJsonifier<GetStageBulkRequestResponseModel>>outputObjectJsonifier):
       TapeAction(accessURL,method,tapeRestApiBusiness), mOutputObjectJsonifier(outputObjectJsonifier){}
  common::HttpResponse * run(common::HttpRequest * request, const common::VirtualIdentity * vid) override;
private:
  std::shared_ptr<TapeRestApiJsonifier<GetStageBulkRequestResponseModel>>  mOutputObjectJsonifier;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_GETSTAGEBULKREQUEST_HH
