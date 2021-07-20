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
#include "mgm/bulk-request/BulkRequestFactory.hh"
#include "mgm/bulk-request/dao/proc/ProcDirBulkRequestFile.hh"

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
  return generateBulkRequestProcPath(bulkRequest->getId(),bulkRequest->getType());
}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(const std::string& bulkRequestId, const BulkRequest::Type& type) {
  return mProcDirectoryBulkRequestLocations.getDirectoryPathWhereBulkRequestCouldBeSaved(type) + "/" + bulkRequestId;
}

void ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath) {
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory");
  const auto & files = *bulkRequest->getFiles();
  //Map of files associated to the future object for the in-memory prefetching of the file informations
  std::map<bulk::File,folly::Future<IFileMDPtr>> filesWithMDFutures;
  for(auto & file : files){
    std::string path = file.first;
    std::pair<bulk::File,folly::Future<IFileMDPtr>> itemToInsert(file.second,mFileSystem->eosView->getFileFut(path , false));
    filesWithMDFutures.emplace(std::move(itemToInsert));
  }
  for(auto & fileMd: filesWithMDFutures){
      fileMd.second.wait();
  }
  for(auto & fileWithMDFuture : filesWithMDFutures){
    const std::string & currentFilePath = fileWithMDFuture.first.getPath();
    std::ostringstream pathOfFileToTouch;
    pathOfFileToTouch << bulkReqProcPath << "/";
    std::shared_ptr<IFileMD> file;
    try {
      eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex,
                                          __FUNCTION__, __LINE__, __FILE__);
      file = mFileSystem->eosView->getFile(currentFilePath);
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
    persistErrorIfAny(pathOfFileToTouch.str(), fileWithMDFuture.first);
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

void ProcDirectoryBulkRequestDAO::persistErrorIfAny(const std::string & persistedFilePath, const bulk::File & file) {
  XrdOucErrInfo errorAttrSet;

  std::optional<std::string> errorMsg = file.getError();
  if(errorMsg) {
    int retAttrSet = mFileSystem->_attr_set(persistedFilePath.c_str(), errorAttrSet, mVid, nullptr, "error_msg", errorMsg.value().c_str());
    if(retAttrSet != SFS_OK) {
      std::ostringstream oss;
      oss << "In ProcDirectoryBulkRequestDAO::persistErrorIfAny(), could not set the error extended attribute to the file path "
          << persistedFilePath << " ErrorMsg=" << errorAttrSet.getErrText();
      eos_debug(oss.str().c_str());
      throw PersistencyException(oss.str());
    }
  }
}

std::unique_ptr<BulkRequest> ProcDirectoryBulkRequestDAO::getBulkRequest(const std::string & id, const BulkRequest::Type & type) {
  std::unique_ptr<BulkRequest> bulkRequest;
  std::string bulkRequestProcPath = this->generateBulkRequestProcPath(id, type);
  if (existsAndIsDirectory(bulkRequestProcPath)) {
    // Directory exists, the bulk-request can be fetched
    bulkRequest.reset(BulkRequestFactory::createBulkRequest(id, type));
    XrdOucErrInfo errFind;
    XrdOucString stdErr;
    std::map<std::string, std::set<std::string>> found;
    if (mFileSystem->_find(bulkRequestProcPath.c_str(), errFind, stdErr, mVid, found) == SFS_OK) {
      // Returns map of directory + filename
      /*std::stringstream ss;
      ss << "findResult=";
      for(auto &kv: found){
        ss << "directory=" << kv.first << "files=[";
        for(auto & fileName: kv.second) {
          ss << fileName << ",";
        }
        ss << "]";
      }
      eos_info(ss.str().c_str());
       */
      auto & bulkReqDirFiles = *found.begin();
      std::string bulkReqDir = bulkReqDirFiles.first;
      std::set<std::string>& filesName = bulkReqDirFiles.second;
      // mFileSystem->eosFileService->getFileMDFut()
      std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> filesInBulkReqProcDirWithFuture;
      std::vector<std::string> fileWithPaths;

      for (auto& fileName : filesName) {
        std::string fullPath = bulkReqDir + fileName;
        ProcDirBulkRequestFile file(fullPath);
        file.setNameInBulkRequestDirectory(fileName);
        // attr ls to get the error
        XrdOucErrInfo error;
        eos::IContainerMD::XAttrMap xattrs;
        if (mFileSystem->_attr_ls(fullPath.c_str(), error, mVid, nullptr,xattrs) == SFS_OK) {
          try {
            file.setError(xattrs.at("error_msg"));
          } catch (const std::out_of_range& ex) {}
          try {
            eos::common::FileId::fileid_t fid = std::stoull(fileName);
            file.setFileId(fid);
            std::pair<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> fileWithFuture(file,mFileSystem->eosFileService->getFileMDFut(fid));
            filesInBulkReqProcDirWithFuture.emplace(std::move(fileWithFuture));
          } catch (std::invalid_argument& ex) {
            // The current file is not a fid, it is therefore a file that has the format #:#eos#:#test#:#testFile.txt (#:# replaced by '/')
            std::string filePathCopy = fileName;
            common::StringConversion::ReplaceStringInPlace(filePathCopy, "#:#",
                                                           "/");
            File bulkRequestFile(filePathCopy);
            bulkRequestFile.setError(file.getError());
            bulkRequest->addFile(bulkRequestFile);
          }
        }
      }
      for(auto &fileWithFuture: filesInBulkReqProcDirWithFuture) {
        fileWithFuture.second.wait();
        eos::common::RWMutexReadLock lock(mFileSystem->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
        std::shared_ptr<IFileMD> fmd = gOFS->eosFileService->getFileMD(fileWithFuture.first.getFileId().value());
        File bulkReqFile(mFileSystem->eosView->getUri(fmd.get()));
        bulkReqFile.setError(fileWithFuture.first.getError());
        bulkRequest->addFile(bulkReqFile);
      }
    }
  }
  return bulkRequest;
}

bool ProcDirectoryBulkRequestDAO::existsAndIsDirectory(const std::string& dirPath) {
  XrdOucErrInfo error;
  XrdSfsFileExistence fileExistence;
  int retCode = mFileSystem->_exists(dirPath.c_str(),fileExistence,error,mVid);
  if(retCode != SFS_OK) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::existsAndIsDirectory(), could not get information about the existence of the directory "
        << dirPath << " ErrorMsg=" << error.getErrText();
    eos_debug(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
  return fileExistence == XrdSfsFileExistIsDirectory;
}

EOSBULKNAMESPACE_END