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

StageBulkRequest * BulkRequestFactory::createStageBulkRequest() {
  std::string bulkRequestId = BulkRequestHelper::generateBulkRequestId();
  return static_cast<StageBulkRequest *>(createBulkRequest(bulkRequestId,BulkRequest::Type::PREPARE_STAGE));
}

EvictBulkRequest * BulkRequestFactory::createEvictBulkRequest() {
  std::string bulkRequestId = BulkRequestHelper::generateBulkRequestId();
  return static_cast<EvictBulkRequest *>(createBulkRequest(bulkRequestId,BulkRequest::Type::PREPARE_EVICT));
}

BulkRequest * BulkRequestFactory::createBulkRequest(const std::string& id, const BulkRequest::Type & type) {
  switch(type){
  case BulkRequest::Type::PREPARE_STAGE:
    return new StageBulkRequest(id);
  case BulkRequest::Type::PREPARE_EVICT:
    return new EvictBulkRequest(id);
  default:
    return nullptr;
  }
}
EOSBULKNAMESPACE_END
