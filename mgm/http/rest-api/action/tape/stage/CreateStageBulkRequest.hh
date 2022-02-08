// ----------------------------------------------------------------------
// File: CreateStageBulkRequest.hh
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


#ifndef EOS_CREATESTAGEBULKREQUEST_HH
#define EOS_CREATESTAGEBULKREQUEST_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/action/tape/TapeAction.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include "mgm/http/rest-api/json/builder/JsonModelBuilder.hh"
#include "mgm/http/rest-api/business/tape/ITapeRestApiBusiness.hh"
#include "mgm/http/rest-api/json/tape/TapeRestApiJsonifier.hh"
#include "mgm/http/rest-api/model/tape/stage/CreatedStageBulkRequestResponseModel.hh"

EOSMGMRESTNAMESPACE_BEGIN

class CreateStageBulkRequest : public TapeAction {
public:
  CreateStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method,std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness, std::shared_ptr<JsonModelBuilder<CreateStageBulkRequestModel>> inputJsonModelBuilder,std::shared_ptr<
          TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>> outputObjectJsonifier): TapeAction(accessURL,method,tapeRestApiBusiness),mInputJsonModelBuilder(inputJsonModelBuilder),
        mOutputObjectJsonifier(outputObjectJsonifier){}
  common::HttpResponse * run(common::HttpRequest * request, const common::VirtualIdentity * vid) override;
private:
  std::shared_ptr<JsonModelBuilder<CreateStageBulkRequestModel>> mInputJsonModelBuilder;
  std::shared_ptr<TapeRestApiJsonifier<CreatedStageBulkRequestResponseModel>> mOutputObjectJsonifier;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CREATESTAGEBULKREQUEST_HH
