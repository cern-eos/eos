//------------------------------------------------------------------------------
//! @file CancellationBulkRequest.hh
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

#ifndef EOS_CANCELLATIONBULKREQUEST_HH
#define EOS_CANCELLATIONBULKREQUEST_HH

#include "mgm/Namespace.hh"
#include "mgm/bulk-request/BulkRequest.hh"

EOSBULKNAMESPACE_BEGIN

/**
 * This class represents a bulk request containing files that
 * have to be prepared
 */
class CancellationBulkRequest : public BulkRequest
{
public:
  CancellationBulkRequest(const std::string& id);
  BulkRequest::Type getType() const override;
private:
};

EOSBULKNAMESPACE_END
#endif // EOS_CANCELLATIONBULKREQUEST_HH
