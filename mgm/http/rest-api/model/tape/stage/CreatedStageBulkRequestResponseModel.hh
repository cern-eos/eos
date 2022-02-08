// ----------------------------------------------------------------------
// File: CreatedStageBulkRequestResponseModel.hh
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

#ifndef EOS_CREATEDSTAGEBULKREQUESTRESPONSEMODEL_HH
#define EOS_CREATEDSTAGEBULKREQUESTRESPONSEMODEL_HH

#include "mgm/Namespace.hh"
#include <cstdint>
#include <string>
#include "mgm/bulk-request/BulkRequest.hh"
#include "common/json/Jsonifiable.hh"

EOSMGMRESTNAMESPACE_BEGIN

class CreatedStageBulkRequestResponseModel : public common::Jsonifiable<CreatedStageBulkRequestResponseModel> {
public:
  CreatedStageBulkRequestResponseModel(/*const std::string & jsonFromClient, */const std::string & accessURL);
  const std::string & getAccessURL() const;
  //const std::string & getJsonRequest() const;
private:
  //const std::string & mJsonFromClient;
  const std::string & mAccessURL;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CREATEDSTAGEBULKREQUESTRESPONSEMODEL_HH
