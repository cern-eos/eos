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

/**
 * This object is the model that will be returned to the client
 * after a STAGE bulk-request submission via the tape REST API
 */
class CreatedStageBulkRequestResponseModel : public
  common::Jsonifiable<CreatedStageBulkRequestResponseModel>
{
public:
  inline CreatedStageBulkRequestResponseModel(const std::string& requestId)
    : Jsonifiable(), mRequestId(requestId) {}
  inline const std::string& getRequestId() const { return mRequestId; }
private:
  const std::string mRequestId;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_CREATEDSTAGEBULKREQUESTRESPONSEMODEL_HH
