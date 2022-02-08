//------------------------------------------------------------------------------
//! @file BulkRequestBusiness.cc
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

#include "BulkRequestBusiness.hh"
#include "mgm/Stat.hh"
#include "mgm/XrdMgmOfs.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequestBusiness::BulkRequestBusiness(std::unique_ptr<AbstractDAOFactory> && daoFactory) : mDaoFactory(std::move(daoFactory)){
}

void BulkRequestBusiness::saveBulkRequest(const std::shared_ptr<BulkRequest> req){
  eos_info("msg=\"Persisting bulk request id=%s nbFiles=%ld type=%s\"",req->getId().c_str(), req->getFiles()->size(),BulkRequest::bulkRequestTypeToString(req->getType()).c_str());
  EXEC_TIMING_BEGIN("BulkRequestBusiness::saveBulkRequest");
  mDaoFactory->getBulkRequestDAO()->saveBulkRequest(req);
  EXEC_TIMING_END("BulkRequestBusiness::saveBulkRequest");
  eos_info("msg=\"Persisted bulk request id=%s\"",req->getId().c_str());
}

std::unique_ptr<BulkRequest> BulkRequestBusiness::getBulkRequest(const std::string & bulkRequestId, const BulkRequest::Type & type){
  EXEC_TIMING_BEGIN("BulkRequestBusiness::getBulkRequest");
  std::unique_ptr<BulkRequest> bulkRequest =  mDaoFactory->getBulkRequestDAO()->getBulkRequest(bulkRequestId,type);
  EXEC_TIMING_END("BulkRequestBusiness::getBulkRequest");
  if(bulkRequest != nullptr) {
    eos_info("msg=\"Retrieved bulk request id=%s from persistence layer\"",
             bulkRequestId.c_str());
  } else {
    eos_info("msg=\"No bulk request with id=%s has been found in the persistence layer\"",
             bulkRequestId.c_str());
  }
  return bulkRequest;
}

void BulkRequestBusiness::addOrUpdateAttributes(const std::shared_ptr<BulkRequest> req, const std::map<std::string, std::string>& attributes){
  EXEC_TIMING_BEGIN("BulkRequestBusiness::addOrUpdateAttributes");
  mDaoFactory->getBulkRequestDAO()->addOrUpdateAttributes(req,attributes);
  EXEC_TIMING_END("BulkRequestBusiness::addOrUpdateAttributes");
}

bool BulkRequestBusiness::exists(const std::string& bulkRequestId, const BulkRequest::Type& type) {
  return mDaoFactory->getBulkRequestDAO()->exists(bulkRequestId, type);
}

void BulkRequestBusiness::deleteBulkRequest(const std::shared_ptr<BulkRequest> req) {
  return mDaoFactory->getBulkRequestDAO()->deleteBulkRequest(req);
}

EOSBULKNAMESPACE_END