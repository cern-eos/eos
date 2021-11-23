//------------------------------------------------------------------------------
//! @file BulkRequestProcCleaner.cc
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

#include "BulkRequestProcCleaner.hh"
#include "mgm/IMaster.hh"
#include "mgm/XrdMgmOfs.hh"
#include <common/IntervalStopwatch.hh>
#include "mgm/bulk-request/dao/factories/AbstractDAOFactory.hh"
#include "mgm/bulk-request/dao/factories/ProcDirectoryDAOFactory.hh"
#include "mgm/bulk-request/dao/IBulkRequestDAO.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"

EOSBULKNAMESPACE_BEGIN

BulkRequestProcCleaner::BulkRequestProcCleaner(const bulk::ProcDirectoryBulkRequestLocations & bulkReqDirectory, std::unique_ptr<BulkRequestProcCleanerConfig> config): mBulkRequestLocation(bulkReqDirectory),mConfig(std::move(config)){

}

void
BulkRequestProcCleaner::Start(){
  mThread.reset(&BulkRequestProcCleaner::backgroundThread,this);
}

void
BulkRequestProcCleaner::Stop(){
  mThread.join();
}

void
BulkRequestProcCleaner::backgroundThread(ThreadAssistant & assistant){
  std::ostringstream oss;
  oss << "msg=\"starting BulkRequestProcCleaner thread. Directory=" << mBulkRequestLocation.getBulkRequestDirectory() <<  "\"";
  eos_static_notice(oss.str().c_str());
  gOFS->WaitUntilNamespaceIsBooted(assistant);
  // Wait that current MGM becomes a master
  do {
    eos_static_debug("%s", "msg=\"BulkRequestProcCleaner waiting for master MGM\"");
    assistant.wait_for(std::chrono::seconds(10));
  } while (!assistant.terminationRequested() && !gOFS->mMaster->IsMaster());

  while (!assistant.terminationRequested()) {
    // every now and then we wake up
    common::IntervalStopwatch stopwatch(mConfig->mInterval);
    // Only a master needs to run the cleaner
    if (gOFS->mMaster->IsMaster()) {
      //Initialize the bulk-request DAO
      std::unique_ptr<AbstractDAOFactory> daoFactory(new ProcDirectoryDAOFactory(gOFS,mBulkRequestLocation));
      std::unique_ptr<IBulkRequestDAO> bulkReqDao = daoFactory->getBulkRequestDAO();
      try{
        uint64_t nbBulkRequestDeleted = bulkReqDao->deleteBulkRequestNotQueriedFor(BulkRequest::Type::PREPARE_STAGE,mConfig->mBulkReqLastAccessTimeBeforeCleaning);
        eos_static_info("msg=\"BulkRequestProcCleaner did one round of cleaning, nbDeletedBulkRequests=%ld\"",nbBulkRequestDeleted);
      } catch (const PersistencyException &ex) {
        eos_static_err("msg=\"BulkRequestProcCleaner an exception occured during a round of cleaning\" exceptionMsg=\"%s\"",ex.what());
      }
    }

    while (stopwatch.timeRemainingInCycle() >= std::chrono::seconds(5)) {
      assistant.wait_for(std::chrono::seconds(5));

      if (assistant.terminationRequested()) {
        break;
      }
    }
  }
}

BulkRequestProcCleaner::~BulkRequestProcCleaner(){
  Stop();
}

EOSBULKNAMESPACE_END
