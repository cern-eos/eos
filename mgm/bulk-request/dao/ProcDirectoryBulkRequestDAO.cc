//------------------------------------------------------------------------------
//! @file ProcDirectoryBulkRequestDAO.hh
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

#include "ProcDirectoryBulkRequestDAO.hh"
#include "mgm/Stat.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include <common/StringConversion.hh>
#include <namespace/Prefetcher.hh>
#include "namespace/interface/IView.hh"
#include "mgm/proc/ProcCommand.hh"

EOSBULKNAMESPACE_BEGIN

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(XrdMgmOfs * fileSystem):mFileSystem(fileSystem){

}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const std::shared_ptr<StageBulkRequest> bulkRequest) {
  mVid = common::VirtualIdentity::Root();
  std::string directoryBulkReqPath = generateBulkRequestProcPath(bulkRequest);
  try {
    createBulkRequestDirectory(bulkRequest,directoryBulkReqPath);
    insertBulkRequestFilesToBulkRequestDirectory(bulkRequest,directoryBulkReqPath);
  } catch (const PersistencyException &ex){
    cleanAfterExceptionHappenedDuringBulkRequestSave(bulkRequest,directoryBulkReqPath);
    throw ex;
  }
}

void ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest,const std::string & bulkReqProcPath) {
  eos_info("msg=\"Persistence of the bulk request %s : creating the directory %s\"",bulkRequest->getId().c_str(),
           bulkReqProcPath.c_str());
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::createBulkRequestDirectory");
  XrdOucErrInfo error;
  int directoryCreationRetCode = mFileSystem->_mkdir(bulkReqProcPath.c_str(),S_IFDIR | S_IRWXU,error, mVid);
  if(directoryCreationRetCode != SFS_OK){
    std::ostringstream errMsg;
    errMsg << "In ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(), could not create the directory to save the bulk-request id=" << bulkRequest->getId() <<" ErrorMsg=" << error.getErrText();
    throw PersistencyException(errMsg.str());
  }
  EXEC_TIMING_END("ProcDirectoryBulkRequestDAO::createBulkRequestDirectory");
}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(const std::shared_ptr<BulkRequest> bulkRequest) {
  XrdOucString directoryBulkRequest = mFileSystem->MgmProcBulkRequestPath + "/" + bulkRequest->getId().c_str();
  return directoryBulkRequest.c_str();
}

void ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath) {
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory");
  const std::set<std::string> & paths = bulkRequest->getPaths();
  eos::Prefetcher prefetcher(mFileSystem->eosView);
  for(auto & path : paths){
    prefetcher.stageFileMD(path,false);
  }
  prefetcher.wait();
  for(auto & path: paths){
    std::shared_ptr<IFileMD> file;
    {
      try {
        eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex,
                                            __FUNCTION__, __LINE__, __FILE__);
        file = mFileSystem->eosView->getFile(path);
      } catch(const std::exception &ex){
        std::ostringstream errMsg;
        errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), got an exception trying to get informations about the file "
               << path << " ErrorMsg=" << ex.what();
        throw PersistencyException(ex.what());
      }
    }
    std::ostringstream fullPath;
    fullPath << bulkReqProcPath << "/" << file->getId();
    XrdOucErrInfo error;
    int retTouch =
        mFileSystem->_touch(fullPath.str().c_str(), error, mVid);
    if (retTouch != SFS_OK) {
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), could not create the file to save the file "
             << path << " that belongs to the bulk-request id="
             << bulkRequest->getId() << " ErrorMsg=" << error.getErrText();
      throw PersistencyException(errMsg.str());
    }
  }
  EXEC_TIMING_END("ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory");
}

void ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave(const std::shared_ptr<BulkRequest> bulkRequest, const std::string& bulkReqProcPath) {
  //Check first if the directory has been created
  struct stat buf;

  XrdOucErrInfo lError;

  if (gOFS->_stat(bulkReqProcPath.c_str(), &buf, lError, mVid, "", nullptr, false)) {
    //If we cannot find the directory, there's nothing we can do.
    std::ostringstream debugMsg;
    debugMsg << "In ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave() "
        << "the directory " << bulkReqProcPath << " does not exist. Nothing to clean";
    eos_debug(debugMsg.str().c_str());
    return;
  }

    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;

    // we do a recursive deletion
    info = "mgm.cmd=rm&mgm.option=r&mgm.retc=1&mgm.path=";

    info += bulkReqProcPath.c_str();
    int result = Cmd.open("/proc/user", info.c_str(), mVid, &lError);

    Cmd.close();

    if(result == SFS_ERROR){
      std::ostringstream debugMsg;
      debugMsg << "In ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave() "
               << "unable to clean the directory " << bulkReqProcPath << "ErrorMsg=" << lError.getErrText();
      eos_debug(debugMsg.str().c_str());
    }
}

EOSBULKNAMESPACE_END