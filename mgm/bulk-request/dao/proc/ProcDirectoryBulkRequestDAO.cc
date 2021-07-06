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

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(XrdMgmOfs * fileSystem, ProcDirectoryBulkRequestLocations& procDirectoryBulkRequestLocations):mFileSystem(fileSystem),
      mProcDirectoryBulkRequestLocations(procDirectoryBulkRequestLocations),mVid(common::VirtualIdentity::Root()){

}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const std::shared_ptr<BulkRequest> bulkRequest) {
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
  return mProcDirectoryBulkRequestLocations.getDirectoryPathToSaveBulkRequest(*bulkRequest) + "/" + bulkRequest->getId();
}

void ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath) {
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory");
  const auto & files = *bulkRequest->getFiles();
  //Map of files associated to the future object for the in-memory prefetching of the file informations
  std::map<std::string,folly::Future<IFileMDPtr>> pathsWithMDFutures;
  for(auto & file : files){
    std::string path = file.first;
    std::pair<std::string,folly::Future<IFileMDPtr>> itemToInsert(path,mFileSystem->eosView->getFileFut(path , false));
    pathsWithMDFutures.emplace(std::move(itemToInsert));
  }
  for(auto & fileMd: pathsWithMDFutures){
      fileMd.second.wait();
  }
  for(auto & pathWithMDFuture : pathsWithMDFutures){
    const std::string & currentFilePath = pathWithMDFuture.first;
    std::ostringstream pathOfFileToTouch;
    pathOfFileToTouch << bulkReqProcPath << "/";
    std::shared_ptr<IFileMD> file;
    try {
      eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex,
                                          __FUNCTION__, __LINE__, __FILE__);
      file = mFileSystem->eosView->getFile(pathWithMDFuture.first);
      pathOfFileToTouch << file->getId();
    } catch (const eos::MDException &ex){
      //The file does not exist, we will store the path under the same format as it is in the recycle bin
      std::string newFilePath = currentFilePath;
      common::StringConversion::ReplaceStringInPlace(newFilePath,"/","#:#");
      pathOfFileToTouch << newFilePath;
    } catch(const std::exception &ex){
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), got a standard exception trying to get informations about the file "
             << currentFilePath << " ExceptionWhat=" << ex.what();
      throw PersistencyException(errMsg.str());
    }
    XrdOucErrInfo error;
    int retTouch =
        mFileSystem->_touch(pathOfFileToTouch.str().c_str(), error, mVid);
    if (retTouch != SFS_OK) {
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), could not create the file to save the file "
             <<  currentFilePath << " that belongs to the bulk-request id="
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

  if (mFileSystem->_stat(bulkReqProcPath.c_str(), &buf, lError, mVid, "", nullptr, false)) {
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