//------------------------------------------------------------------------------
//! @file BulkRequestPrepareManager.cc
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

#include "BulkRequestPrepareManager.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/bulk-request/exception/BulkRequestException.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequestPrepareManager::BulkRequestPrepareManager(IMgmFileSystemInterface& mgmFsInterface):
  PrepareManager(mgmFsInterface){
}

void BulkRequestPrepareManager::setBulkRequestBusiness(std::shared_ptr<BulkRequestBusiness> bulkRequestBusiness) {
  mBulkRequestBusiness = bulkRequestBusiness;
}

std::shared_ptr<BulkRequest> BulkRequestPrepareManager::getBulkRequest() const {
  return mBulkRequest;
}

void BulkRequestPrepareManager::initializeStagePrepareRequest(XrdOucString& reqid){
  mBulkRequest.reset(BulkRequestFactory::createStageBulkRequest());
  reqid = mBulkRequest->getId().c_str();
}

void BulkRequestPrepareManager::initializeEvictPrepareRequest(XrdOucString& reqid) {
  mBulkRequest.reset(BulkRequestFactory::createEvictBulkRequest());
  reqid = mBulkRequest->getId().c_str();
}

void BulkRequestPrepareManager::setErrorToBulkRequest(const std::string& path, const std::string& error) {
  try {
    if(mBulkRequest != nullptr)
      mBulkRequest->addError(path, error);
  } catch(const BulkRequestException &ex) {
    std::ostringstream oss;
    oss << "msg=\"Unable to add an error to the path " << path << " in the bulk-request " << mBulkRequest->getId() << "\" ExceptionMsg=\"" << ex.what() << "\"";
    eos_warning(oss.str().c_str());
  }
}

void BulkRequestPrepareManager::addPathToBulkRequest(const std::string& path) {
  if(mBulkRequest != nullptr) {
    mBulkRequest->addPath(path);
  }
}

void BulkRequestPrepareManager::saveBulkRequest() {
  if(mBulkRequestBusiness != nullptr && mBulkRequest != nullptr){
    try {
      mBulkRequestBusiness->saveBulkRequest(mBulkRequest);
    } catch(const PersistencyException & ex){
      eos_err("msg=\"Unable to persist the bulk request %s\" \"ExceptionWhat=%s\"",mBulkRequest->getId().c_str(),ex.what());
      throw ex;
    }
  }
}

const std::shared_ptr<FileCollection::Files> BulkRequestPrepareManager::getFileCollectionFromPersistency(const std::string& reqid) {
  std::shared_ptr<FileCollection::Files> ret(new FileCollection::Files());
  if(mBulkRequestBusiness != nullptr){
    std::unique_ptr<BulkRequest> bulkRequest = mBulkRequestBusiness->getBulkRequest(reqid,BulkRequest::PREPARE_STAGE);
    if(bulkRequest != nullptr){
      ret = bulkRequest->getFiles();
    }
  }
  return ret;
}

EOSBULKNAMESPACE_END