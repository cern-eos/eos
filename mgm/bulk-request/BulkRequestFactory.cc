//------------------------------------------------------------------------------
//! @file BulkRequestFactory.cc
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

#include "BulkRequestFactory.hh"
#include "BulkRequestHelper.hh"

EOSBULKNAMESPACE_BEGIN

std::unique_ptr<StageBulkRequest> BulkRequestFactory::createStageBulkRequest(
  const common::VirtualIdentity& issuerVid)
{
  std::string bulkRequestId = BulkRequestHelper::generateBulkRequestId();
  return std::make_unique<StageBulkRequest>(bulkRequestId, issuerVid);
}

std::unique_ptr<StageBulkRequest> BulkRequestFactory::createStageBulkRequest(
  const std::string& requestId, const common::VirtualIdentity& issuerVid)
{
  return std::make_unique<StageBulkRequest>(requestId, issuerVid);
}

std::unique_ptr<StageBulkRequest> BulkRequestFactory::createStageBulkRequest(
  const std::string& requestId, const common::VirtualIdentity& issuerVid,
  const time_t& creationTime)
{
  return std::make_unique<StageBulkRequest>(requestId, issuerVid, creationTime);
}

std::unique_ptr<CancellationBulkRequest>
BulkRequestFactory::createCancelBulkRequest(const std::string& id)
{
  return std::make_unique<CancellationBulkRequest>(id);
}

EOSBULKNAMESPACE_END
