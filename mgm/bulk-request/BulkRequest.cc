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
#include "common/exception/Exception.hh"
#include <sstream>
#include "mgm/bulk-request/exception/BulkRequestException.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequest::BulkRequest(const std::string & id):mId(id){
}

const std::string BulkRequest::getId() const {
  return mId;
}

void BulkRequest::addPath(const std::string & path) {
  mFileCollection.addFile(path);
}

void BulkRequest::addError(const std::string &path, const std::string & error) {
  try {
    mFileCollection.addError(path, error);
  } catch(const common::Exception &ex){
    std::ostringstream oss;
    oss << "In BulkRequest::addError(), an exception occured. ExceptionWhat=" << ex.what();
    throw BulkRequestException(oss.str());
  }
}

void BulkRequest::addFile(const File & file){
  mFileCollection.addFile(file);
}

const std::shared_ptr<FileCollection::Files> BulkRequest:: getFiles() const
{
  return mFileCollection.getAllFiles();
}

const std::shared_ptr<std::set<File>> BulkRequest::getAllFilesInError() const {
  return mFileCollection.getAllFilesInError();
}

const std::string BulkRequest::bulkRequestTypeToString(const BulkRequest::Type & bulkRequestType){
  return BulkRequest::BULK_REQ_TYPE_TO_STRING_MAP.at(bulkRequestType);
}

const std::map<BulkRequest::Type,std::string> BulkRequest::BULK_REQ_TYPE_TO_STRING_MAP = {
    {BulkRequest::PREPARE_STAGE,"PREPARE_STAGE"},
    {BulkRequest::PREPARE_EVICT,"PREPARE_EVICT"}
};


EOSBULKNAMESPACE_END