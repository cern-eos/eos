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
#include <mgm/bulk-request/dao/ProcDirectoryBulkRequestDAO.hh>
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include "mgm/Stat.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequestBusiness::BulkRequestBusiness(std::unique_ptr<AbstractDAOFactory> && daoFactory) : mDaoFactory(std::move(daoFactory)){
}

void BulkRequestBusiness::saveBulkRequest(const std::shared_ptr<BulkRequest> req){
  eos_info("msg=\"Persisting bulk request id=%s nbFiles=%ld type=%s\"",req->getId().c_str(),req->getPaths().size(),BulkRequest::bulkRequestTypeToString(req->getType()).c_str());
  EXEC_TIMING_BEGIN("BulkRequestBusiness::saveBulkRequest");
  dispatchBulkRequestSave(req);
  EXEC_TIMING_END("BulkRequestBusiness::saveBulkRequest");
  eos_info("msg=\"Persisted bulk request id=%s\"",req->getId().c_str());
}

void BulkRequestBusiness::dispatchBulkRequestSave(const std::shared_ptr<BulkRequest> req) {
  switch(req->getType()) {
  case BulkRequest::PREPARE_STAGE:
    mDaoFactory->getBulkRequestDAO()->saveBulkRequest(static_pointer_cast<StageBulkRequest>(req));
    break;
  default:
    break;
  }
}

EOSBULKNAMESPACE_END