// ----------------------------------------------------------------------
// File: CancelStageBulkRequest.hh
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
#ifndef EOS_CANCELSTAGEBULKREQUEST_HH
#define EOS_CANCELSTAGEBULKREQUEST_HH

#include "mgm/Namespace.hh"
#include "mgm/http/rest-api/action/tape/TapeAction.hh"
#include "mgm/http/rest-api/json/ModelBuilder.hh"
#include "mgm/http/rest-api/model/tape/stage/PathsModel.hh"
#include "mgm/http/rest-api/business/tape/TapeRestApiBusiness.hh"

EOSMGMRESTNAMESPACE_BEGIN

class CancelStageBulkRequest : public TapeAction {
public:
  CancelStageBulkRequest(const std::string & accessURL,const common::HttpHandler::Methods method,std::shared_ptr<ITapeRestApiBusiness> tapeRestApiBusiness,std::shared_ptr<ModelBuilder<PathsModel>> inputJsonModelBuilder):
    TapeAction(accessURL,method,tapeRestApiBusiness),mInputJsonModelBuilder(inputJsonModelBuilder){}
  common::HttpResponse * run(common::HttpRequest * request, const common::VirtualIdentity * vid) override;
private:
  std::shared_ptr<ModelBuilder<PathsModel>> mInputJsonModelBuilder;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CANCELSTAGEBULKREQUEST_HH
