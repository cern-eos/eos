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

EOSMGMNAMESPACE_BEGIN

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(XrdMgmOfs * fileSystem):mFileSystem(fileSystem){

}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const std::shared_ptr<StageBulkRequest> bulkRequest) {
  std::string directoryBulkReqPath = generateBulkRequestProcPath(bulkRequest);
  createBulkRequestDirectory(bulkRequest,directoryBulkReqPath);
  insertBulkRequestFilesToBulkRequestDirectory(bulkRequest,directoryBulkReqPath);
}

void ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest,const std::string & bulkReqProcPath) {
  eos_info("msg=\"Persistence of the bulk request %s : creating the directory %s\"",bulkRequest->getId().c_str(),
           bulkReqProcPath.c_str());
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::createBulkRequestDirectory");
  XrdOucErrInfo error;
  eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
  int directoryCreationRetCode = mFileSystem->_mkdir(bulkReqProcPath.c_str(),S_IFDIR | S_IRWXU,error,rootvid);
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
  for(auto & path : bulkRequest->getPaths()){
    eos_info("msg=\"Persistence of the bulk request file=%s\"", path.c_str());
    std::string transformedPath = transformPathForInsertionInDirectory(path);
    eos_info("msg=\"Persistence of the bulk request file transformed=%s\"", transformedPath.c_str());
    eos::common::VirtualIdentity rootvid = eos::common::VirtualIdentity::Root();
    XrdOucErrInfo error;
    std::string fullPath = bulkReqProcPath + "/" + transformedPath;
    int retTouch = mFileSystem->_touch(fullPath.c_str(),error,rootvid);
    if(retTouch != SFS_OK){
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), could not create the directory to save the bulk-request id=" << bulkRequest->getId() <<" ErrorMsg=" << error.getErrText();
      throw PersistencyException(errMsg.str());
    }
  }
  EXEC_TIMING_END("ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory");
}

std::string ProcDirectoryBulkRequestDAO::transformPathForInsertionInDirectory(const std::string& path){
  std::string ret(path);
  eos::common::StringConversion::ReplaceStringInPlace(ret,"/","#:#");
  return ret;
}

EOSMGMNAMESPACE_END