// ----------------------------------------------------------------------
// File: ITapeRestApiBusiness.hh
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

#ifndef EOS_ITAPERESTAPIBUSINESS_HH
#define EOS_ITAPERESTAPIBUSINESS_HH

#include "mgm/Namespace.hh"
#include <memory>
#include "mgm/bulk-request/BulkRequest.hh"
#include "mgm/http/rest-api/model/tape/stage/CreateStageBulkRequestModel.hh"
#include "common/VirtualIdentity.hh"

EOSMGMRESTNAMESPACE_BEGIN

class ITapeRestApiBusiness {
public:
  virtual std::shared_ptr<bulk::BulkRequest> createStageBulkRequest(const CreateStageBulkRequestModel * model, const common::VirtualIdentity * vid) = 0;
};

EOSMGMRESTNAMESPACE_END

#endif // EOS_ITAPERESTAPIBUSINESS_HH
