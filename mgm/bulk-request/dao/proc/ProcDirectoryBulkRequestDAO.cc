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

EOSBULKNAMESPACE_BEGIN

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(XrdMgmOfs * fileSystem, const ProcDirectoryBulkRequestLocations& procDirectoryBulkRequestLocations):mFileSystem(fileSystem),
      mProcDirectoryBulkRequestLocations(procDirectoryBulkRequestLocations),mVid(common::VirtualIdentity::Root()){

}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const std::shared_ptr<BulkRequest> bulkRequest) {
  std::string directoryBulkReqPath = generateBulkRequestProcPath(bulkRequest);
  try {
    if(bulkRequest->getFiles()->size() == 0) {
      std::ostringstream oss;
      oss << "In ProcDirectoryBulkRequestDAO::saveBulkRequest(), unable to persist the bulk-request id=" << bulkRequest->getId() << " because it does not contain any files";
      throw PersistencyException(oss.str());
    }

    //The bulk-request directory will have one extended attribute per file belonging to the bulk-request
    //The persistency consists of creating the directory, and set the extended attribute representing each file
    //The key of the extended attribute will be the fid of the file, the value will be the eventual error that
    //a file can have (prepare submission error...)

    eos_debug("msg=\"Persistence of the bulk request %s : creating the directory %s\"",bulkRequest->getId().c_str(),
       directoryBulkReqPath.c_str());
    createBulkRequestDirectory(bulkRequest,directoryBulkReqPath);
    eos_debug("msg=\"Persistence of the bulk request %s : creating the xattrs map from the bulk-request paths\"",bulkRequest->getId().c_str());
    eos::IContainerMD::XAttrMap xattrs;
    generateXattrsMapFromBulkRequestFiles(bulkRequest, xattrs);
    eos_debug("msg=\"Persistence of the bulk request %s : persisting the bulk-request information in the directory %s\"",bulkRequest->getId().c_str(),directoryBulkReqPath.c_str());
    persistBulkRequestDirectory(directoryBulkReqPath,xattrs);

  } catch (const PersistencyException &ex){
    cleanAfterExceptionHappenedDuringBulkRequestSave(directoryBulkReqPath);
    throw ex;
  }
}

void ProcDirectoryBulkRequestDAO::generateXattrsMapFromBulkRequestFiles(const std::shared_ptr<BulkRequest> bulkRequest, eos::IContainerMD::XAttrMap& xattrs) {
  std::map<bulk::File,folly::Future<IFileMDPtr>> filesWithMDFutures;

  const auto & files = *bulkRequest->getFiles();
  for(auto & file : files){
    std::string path = file.first;
    std::pair<bulk::File,folly::Future<IFileMDPtr>> itemToInsert(file.second,mFileSystem->eosView->getFileFut(path , false));
    filesWithMDFutures.emplace(std::move(itemToInsert));
  }
  for(auto & fileMd: filesWithMDFutures){
    fileMd.second.wait();
  }
  for(auto & fileWithMDFuture : filesWithMDFutures) {
    const std::string& currentFilePath = fileWithMDFuture.first.getPath();
    std::shared_ptr<IFileMD> file;
    std::string fid;
    std::string fileError;
    try {
      eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex,
                                          __FUNCTION__, __LINE__, __FILE__);
      file = mFileSystem->eosView->getFile(currentFilePath);
      fid = std::to_string(file->getId());
    } catch (const eos::MDException &ex){
      //The file does not exist, we will store the path under the same format as it is in the recycle bin
      std::string newFilePath = currentFilePath;
      common::StringConversion::ReplaceStringInPlace(newFilePath,"/","#:#");
      fid = newFilePath;
    } catch(const std::exception &ex){
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::generateXattrsMapFromBulkRequestFiles(), got a standard exception trying to get informations about the file "
             << currentFilePath << " ExceptionWhat=\"" << ex.what() << "\"";
      throw PersistencyException(errMsg.str());
    }
    //Prepend the file id / modified path of the file with the fid identifier
    xattrs[FILE_ID_PREFIX_XATTR + fid] = fileWithMDFuture.first.getError() ? fileWithMDFuture.first.getError().value() : "";
  }
}

void ProcDirectoryBulkRequestDAO::persistBulkRequestDirectory(const std::string& directoryBulkReqPath, const eos::IContainerMD::XAttrMap& xattrs) {
  std::shared_ptr<eos::IContainerMD> bulkReqDirMd;
  {
    eos::common::RWMutexWriteLock nsLock(mFileSystem->eosViewRWMutex,
                                         __FUNCTION__, __LINE__, __FILE__);
    try {
      bulkReqDirMd = mFileSystem->eosView->getContainer(directoryBulkReqPath);
      for (auto& xattr : xattrs) {
        bulkReqDirMd->setAttribute(xattr.first, xattr.second);
      }
      // Set last access time of the bulk-request directory
      std::time_t now = std::time(nullptr);
      std::string nowStr = std::to_string(now);
      bulkReqDirMd->setAttribute(LAST_ACCESS_TIME_ATTR_NAME, nowStr);
      mFileSystem->eosView->updateContainerStore(bulkReqDirMd.get());
    } catch(const eos::MDException &ex) {
      std::ostringstream oss;
      oss << "In ProcDirectoryBulkRequestDAO::persistBulkRequestDirectory(): unable to persist the bulk-request in the directory " << directoryBulkReqPath
          << "ExceptionWhat=\"" << ex.what() << "\"";
      throw PersistencyException(oss.str());
    }
  }
}

void ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest,const std::string & bulkReqProcPath) {
  XrdOucErrInfo error;
  int directoryCreationRetCode = mFileSystem->_mkdir(bulkReqProcPath.c_str(),S_IFDIR | S_IRWXU,error, mVid);
  if(directoryCreationRetCode != SFS_OK){
    std::ostringstream errMsg;
    errMsg << "In ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(), could not create the directory to save the bulk-request id=" << bulkRequest->getId()
           <<" XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    throw PersistencyException(errMsg.str());
  }

}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(const std::shared_ptr<BulkRequest> bulkRequest) {
  return generateBulkRequestProcPath(bulkRequest->getId(),bulkRequest->getType());
}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(const std::string& bulkRequestId, const BulkRequest::Type& type) {
  return mProcDirectoryBulkRequestLocations.getDirectoryPathWhereBulkRequestCouldBeSaved(type) + bulkRequestId;
}

void ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(const std::shared_ptr<BulkRequest> bulkRequest, const std::string & bulkReqProcPath) {
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
             << currentFilePath << " ExceptionWhat=\"" << ex.what() << "\"";
      throw PersistencyException(errMsg.str());
    }
    {
      //Low level system call
      try {
        std::shared_ptr<IFileMD> fmd;
        {
          eos::common::RWMutexWriteLock(mFileSystem->eosViewRWMutex,
                                        __FUNCTION__, __LINE__, __FILE__);
          auto fmd = mFileSystem->eosView->createFile(pathOfFileToTouch.str(),
                                                      vid.uid, vid.gid);
          fmd->setSize(0);
          if (fileWithMDFuture.first.getError()) {
            fmd->setAttribute(ERROR_MSG_ATTR_NAME,
                              fileWithMDFuture.first.getError().value());
          }
          mFileSystem->eosView->updateFileStore(fmd.get());
        }
      } catch (const eos::MDException &ex) {
        std::ostringstream errMsg;
        errMsg << "In ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(), could not create the file to save the file "
               <<  pathOfFileToTouch.str() << " that belongs to the bulk-request id="
               << bulkRequest->getId() << " ExceptionWhat=\"" << ex.what() << "\"";
        throw PersistencyException(errMsg.str());
      }

    }
  }
  updateLastAccessTime(bulkReqProcPath);
}

void ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave(const std::string& bulkReqProcPath) noexcept {
  try {
    deleteDirectory(bulkReqProcPath);
  } catch(const PersistencyException &ex) {
    std::ostringstream debugMsg;
    debugMsg << "In ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave() "
    << "unable to clean the directory " << bulkReqProcPath << "ErrorMsg=\"" << ex.what() << "\"";
    eos_debug(debugMsg.str().c_str());
  }
}

void ProcDirectoryBulkRequestDAO::deleteDirectory(const std::string & path){
  if (existsAndIsDirectory(path)) {
    // execute a proc command
    ProcCommand Cmd;
    XrdOucString info;

    // we do a recursive deletion
    info = "mgm.cmd=rm&mgm.option=r&mgm.retc=1&mgm.path=";

    info += path.c_str();

    XrdOucErrInfo lError;
    int result = Cmd.open("/proc/user", info.c_str(), mVid, &lError);

    Cmd.close();

    if (result == SFS_ERROR) {
      throw PersistencyException(lError.getErrText());
    }
  }
}

void ProcDirectoryBulkRequestDAO::persistErrorIfAny(const std::string & persistedFilePath, const bulk::File & file) {
  std::optional<std::string> errorMsg = file.getError();
  if(errorMsg) {
    setExtendedAttribute(persistedFilePath,ERROR_MSG_ATTR_NAME,errorMsg.value());
  }
}

std::unique_ptr<BulkRequest> ProcDirectoryBulkRequestDAO::getBulkRequest(const std::string & id, const BulkRequest::Type & type) {
  EXEC_TIMING_BEGIN("ProcDirectoryBulkRequestDAO::getBulkRequest");
  std::unique_ptr<BulkRequest> bulkRequest;
  std::string bulkRequestProcPath = this->generateBulkRequestProcPath(id, type);
  try {
    if (existsAndIsDirectory(bulkRequestProcPath)) {
      // Directory exists, the bulk-request can be fetched
      // Update the last access time of the bulk-request directory
      updateLastAccessTime(bulkRequestProcPath);
      // Fetch the bulk-request
      bulkRequest.reset(BulkRequestFactory::createBulkRequest(id, type));
      fillBulkRequest(bulkRequestProcPath, *bulkRequest);
    }
  } catch(const PersistencyException &ex) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::getBulkRequest(): unable to get the bulk request from the persistency layer "
        << "ErrorMsg=\"" << ex.what() << "\"";
    throw PersistencyException(oss.str());
  }
  EXEC_TIMING_END("ProcDirectoryBulkRequestDAO::getBulkRequest");
  return bulkRequest;
}

bool ProcDirectoryBulkRequestDAO::existsAndIsDirectory(const std::string& dirPath) {
  XrdOucErrInfo error;
  XrdSfsFileExistence fileExistence;
  int retCode = mFileSystem->_exists(dirPath.c_str(),fileExistence,error,mVid);
  if(retCode != SFS_OK) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::existsAndIsDirectory(), could not get information about the existence of the directory "
    << dirPath << " XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
  return fileExistence == XrdSfsFileExistIsDirectory;
}

void ProcDirectoryBulkRequestDAO::fillBulkRequest(const std::string & bulkRequestProcPath,BulkRequest& bulkRequest) {
  XrdOucErrInfo errFind;
  XrdOucString stdErr;
  eos::IContainerMD::XAttrMap xattrs;
  fetchExtendedAttributes(bulkRequestProcPath, xattrs);

  std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> filesInBulkReqProcDirWithFuture;
  std::vector<std::string> fileWithPaths;

  for (auto & fileIdError : xattrs) {
    std::string fileId = fileIdError.first;
    size_t pos = fileId.find(FILE_ID_PREFIX_XATTR,0);
    if(pos != std::string::npos){
      fileId.erase(0,FILE_ID_PREFIX_XATTR.length());
    } else {
      continue;
    }
    std::string fullPath = bulkRequestProcPath + fileId;
    //The files in the bulk-request proc directory will be wrapped into a ProcDirBulkRequestFile object.
    ProcDirBulkRequestFile file(fullPath);
    file.setName(fileId);
    // Get the error of the current file if there is some
    if(!fileIdError.second.empty()){
      file.setError(fileIdError.second);
    }

    try {
      //The file name is normally a fid. But if the file submitted before did not exist, the path will be stored in another format (e.g: #:#eos#:#test#:#testFile.txt)
      eos::common::FileId::fileid_t fid = std::stoull(file.getName());
      file.setFileId(fid);
      initiateFileMDFetch(file,filesInBulkReqProcDirWithFuture);
    } catch (std::invalid_argument& ex) {
      // The current file is not a fid, it is therefore a file that has the format #:#eos#:#test#:#testFile.txt (#:# replaced by '/')
      std::string filePathCopy = file.getName();
      common::StringConversion::ReplaceStringInPlace(filePathCopy, "#:#",
                                                     "/");
      File bulkRequestFile(filePathCopy);
      bulkRequestFile.setError(file.getError());
      bulkRequest.addFile(bulkRequestFile);
    }
  }

  getFilesPathAndAddToBulkRequest(filesInBulkReqProcDirWithFuture,bulkRequest);
}

void ProcDirectoryBulkRequestDAO::getDirectoryContent(const std::string & path, std::map<std::string, std::set<std::string>> & directoryContent) {
  XrdOucErrInfo error;
  XrdOucString stdErr;
  if (mFileSystem->_find(path.c_str(), error, stdErr, mVid,directoryContent) == SFS_ERROR) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::getDirectoryContent(), could not list the content of the directory "
    << path << " XrdOfsErrMsg=" << error.getErrText();
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
  //Drop the top directory: it does not belong to its content
  directoryContent.erase(path);
}

void ProcDirectoryBulkRequestDAO::fetchFileExtendedAttributes(const ProcDirBulkRequestFile& file, eos::IContainerMD::XAttrMap & xattrs) {
  fetchExtendedAttributes(file.getFullPath(),xattrs);
}

void ProcDirectoryBulkRequestDAO::fetchExtendedAttributes(const std::string& path, eos::IContainerMD::XAttrMap& xattrs){
  XrdOucErrInfo error;
  if (mFileSystem->_attr_ls(path.c_str(), error, mVid, nullptr,xattrs) == SFS_ERROR) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::fetchExtendedAttributes() Unable to get the extended attribute of the file "
    << path << " XrdOfsErrMsg=" << error.getErrText();
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
}

void ProcDirectoryBulkRequestDAO::fillFileErrorIfAny(ProcDirBulkRequestFile & file, eos::IContainerMD::XAttrMap & xattrs) {
  try {
    //We try to set the error if there were an error set at submission. If no error attribute, the error will not be set
    file.setError(xattrs.at(ERROR_MSG_ATTR_NAME));
  } catch (const std::out_of_range& ex) {}
}

void ProcDirectoryBulkRequestDAO::initiateFileMDFetch(const ProcDirBulkRequestFile& file, std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>>& filesWithFuture) {
  std::pair<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> fileWithFuture(file,mFileSystem->eosFileService->getFileMDFut(file.getFileId().value()));
  filesWithFuture.emplace(std::move(fileWithFuture));
}

void ProcDirectoryBulkRequestDAO::getFilesPathAndAddToBulkRequest(std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> & filesWithFuture, BulkRequest & bulkRequest) {
  eos::common::RWMutexReadLock lock(mFileSystem->eosViewRWMutex, __FUNCTION__, __LINE__, __FILE__);
  for (auto& fileWithFuture : filesWithFuture) {
    try {
      fileWithFuture.second.wait();
      std::shared_ptr<IFileMD> fmd = mFileSystem->eosFileService->getFileMD(fileWithFuture.first.getFileId().value());
      File bulkReqFile(mFileSystem->eosView->getUri(fmd.get()));
      bulkReqFile.setError(fileWithFuture.first.getError());
      bulkRequest.addFile(bulkReqFile);
    } catch (const eos::MDException & ex){
      //We could not get any information about this file (might have been deleted for example)
      //log this as a warning and remove this file
      std::stringstream ss;
      ss << "In ProcDirectoryBulkRequestDAO::getFilesPathAndAddToBulkRequest(), unable to get the metadata of the file id=" << fileWithFuture.first.getFileId().value()
         << " ErrorMsg=\"" << ex.what() << "\"";
      eos_warning(ss.str().c_str());
    }
  }
}

uint64_t ProcDirectoryBulkRequestDAO::deleteBulkRequestNotQueriedFor(const BulkRequest::Type & type, const std::chrono::seconds & seconds) {
  std::string bulkRequestsPath = mProcDirectoryBulkRequestLocations.getDirectoryPathWhereBulkRequestCouldBeSaved(type);
  std::map<std::string, std::set<std::string>> allBulkRequestDirectories;
  getDirectoryContent(bulkRequestsPath,allBulkRequestDirectories);
  //Now get the last access time of each directory
  std::set<std::string> bulkRequestDirectoriesToDelete;
  uint64_t nbDeletedBulkRequests = 0;
  for(auto &kv: allBulkRequestDirectories){
    eos::IContainerMD::XAttrMap xattrs;
    fetchExtendedAttributes(kv.first,xattrs);
    try {
      std::string lastAccessTimeStr = xattrs.at(LAST_ACCESS_TIME_ATTR_NAME);
      std::time_t lastAccessTime = std::atoi(lastAccessTimeStr.c_str());
      time_t elapsedTimeBetweenNowAndLastAccessTime = std::time(nullptr) - lastAccessTime;
      if(elapsedTimeBetweenNowAndLastAccessTime > seconds.count()){
        deleteDirectory(kv.first);
        nbDeletedBulkRequests++;
        eos_info("msg=\"Deleted a bulk request from the /proc/ persistency\" path=\"%s\"",kv.first.c_str());
      }
    } catch (const std::out_of_range &){
      //The extended attribute LAST_ACCESS_TIME_ATTR_NAME was not found, log an error
      eos_err("In ProcDirectoryBulkRequestDAO::deleteBulkRequestNotQueriedFor(), the directory %s does not have the %s extended attribute set. "
              "Unable to know if it can be deleted or not.",kv.first.c_str(),LAST_ACCESS_TIME_ATTR_NAME);
    }
  }
  return nbDeletedBulkRequests;
}

void ProcDirectoryBulkRequestDAO::setExtendedAttribute(const std::string& path, const std::string& xattrName, const std::string& xattrValue){
  XrdOucErrInfo error;
  int retAttrSet = mFileSystem->_attr_set(path.c_str(), error, mVid, nullptr, xattrName.c_str(), xattrValue.c_str());
  if(retAttrSet != SFS_OK) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::setExtendedAttribute(), could not set the extended attribute " << xattrName << " to the file path "
    << path << " XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
}

void ProcDirectoryBulkRequestDAO::updateLastAccessTime(const std::string & path){
  std::time_t now = std::time(nullptr);
  std::string nowStr = std::to_string(now);
  setExtendedAttribute(path,LAST_ACCESS_TIME_ATTR_NAME,nowStr);
}

void ProcDirectoryBulkRequestDAO::addOrUpdateAttributes(const std::shared_ptr<BulkRequest> bulkRequest, const std::map<std::string,std::string> & attributes){
  std::string bulkRequestPath = generateBulkRequestProcPath(bulkRequest);
  if(existsAndIsDirectory(bulkRequestPath)){
    for(const auto& kv: attributes){
      setExtendedAttribute(bulkRequestPath,kv.first,kv.second);
    }
  } else {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::addOrUpdateAttributes(), the bulk-request " << bulkRequest->getId() << " does not exist";
    throw PersistencyException(oss.str());
  }
}

bool ProcDirectoryBulkRequestDAO::exists(const std::string& bulkRequestId, const BulkRequest::Type& type) {
  std::string bulkRequestPath = generateBulkRequestProcPath(bulkRequestId,type);
  return existsAndIsDirectory(bulkRequestPath);
}

EOSBULKNAMESPACE_END