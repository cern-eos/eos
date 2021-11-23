//------------------------------------------------------------------------------
//! @file ProcDirectoryBulkRequestLocations.cc
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

#include "ProcDirectoryBulkRequestLocations.hh"

EOSBULKNAMESPACE_BEGIN

ProcDirectoryBulkRequestLocations::ProcDirectoryBulkRequestLocations(const std::string & procDirectoryPath) {
  mBulkRequestDirectory = procDirectoryPath + "/bulkrequests/";
  mBulkRequestTypeToPath[BulkRequest::Type::PREPARE_STAGE] = mBulkRequestDirectory + "stage/";
  mBulkRequestTypeToPath[BulkRequest::Type::PREPARE_EVICT] = mBulkRequestDirectory + "evict/";
}

std::set<std::string> ProcDirectoryBulkRequestLocations::getAllBulkRequestDirectoriesPath() const{
  std::set<std::string> allBulkRequetsDirectoriesPath;
  for(auto & bulkRequestTypeToPath: mBulkRequestTypeToPath){
    allBulkRequetsDirectoriesPath.insert(bulkRequestTypeToPath.second);
  }
  return allBulkRequetsDirectoriesPath;
}

std::string ProcDirectoryBulkRequestLocations::getBulkRequestDirectory() const{
  return mBulkRequestDirectory;
}

std::string ProcDirectoryBulkRequestLocations::getDirectoryPathWhereBulkRequestCouldBeSaved(const BulkRequest::Type& type) const {
  return mBulkRequestTypeToPath.at(type);
}

EOSBULKNAMESPACE_END
