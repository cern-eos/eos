//------------------------------------------------------------------------------
//! @file StageBulkRequest.hh
//! @author Cedric Caffy - CERN
//------------------------------------------------------------------------------

/************************************************************************
 * EOS - the CERN Disk Storage System                                   *
 * Copyright (C) 2017 CERN/Switzerland                                  *
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
#ifndef EOS_STAGEBULKREQUEST_HH
#define EOS_STAGEBULKREQUEST_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/BulkRequest.hh"

EOSMGMNAMESPACE_BEGIN

/**
 * This class represents a bulk request containing files that
 * have to be prepared
 */
class StageBulkRequest : public BulkRequest {
public:
  StageBulkRequest(const std::string & id, const common::VirtualIdentity & clientVid);
  const BulkRequest::Type getType() const override;
private:
};

EOSMGMNAMESPACE_END
#endif // EOS_STAGEBULKREQUEST_HH
