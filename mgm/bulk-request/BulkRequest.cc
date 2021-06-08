//------------------------------------------------------------------------------
//! @file BulkRequest.cc
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

#include "BulkRequest.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequest::BulkRequest(const std::string & id):mId(id){
}

const std::string BulkRequest::getId() const {
  return mId;
}

void BulkRequest::addPath(const std::string& path) {
  mPaths.insert(path);
}

const std::set<std::string>& BulkRequest::getPaths() const
{
  return mPaths;
}

const std::string BulkRequest::bulkRequestTypeToString(const BulkRequest::Type & bulkRequestType){
  return BulkRequest::BULK_REQ_TYPE_TO_STRING_MAP.at(bulkRequestType);
}

const std::map<BulkRequest::Type,std::string> BulkRequest::BULK_REQ_TYPE_TO_STRING_MAP = {
    {BulkRequest::PREPARE_STAGE,"PREPARE_STAGE"},
    {BulkRequest::PREPARE_EVICT,"PREPARE_EVICT"}
};

EOSBULKNAMESPACE_END