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
#include "mgm/stat/Stat.hh"
#include "mgm/bulk-request/exception/PersistencyException.hh"
#include <common/StringConversion.hh>
#include <namespace/Prefetcher.hh>
#include "namespace/interface/IView.hh"
#include "mgm/proc/ProcCommand.hh"
#include "mgm/bulk-request/BulkRequestFactory.hh"

EOSBULKNAMESPACE_BEGIN

ProcDirectoryBulkRequestDAO::ProcDirectoryBulkRequestDAO(XrdMgmOfs* fileSystem,
    const ProcDirectoryBulkRequestLocations& procDirectoryBulkRequestLocations):
  mFileSystem(fileSystem),
  mProcDirectoryBulkRequestLocations(procDirectoryBulkRequestLocations),
  mVid(common::VirtualIdentity::Root())
{
}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const StageBulkRequest*
    bulkRequest)
{
  std::string directoryBulkReqPath = generateBulkRequestProcPath(bulkRequest);

  try {
    if (bulkRequest->getFiles()->size() == 0) {
      std::ostringstream oss;
      oss << "In ProcDirectoryBulkRequestDAO::saveBulkRequest(), unable to persist the bulk-request id="
          << bulkRequest->getId() << " because it does not contain any files";
      throw PersistencyException(oss.str());
    }

    //The bulk-request directory will have one extended attribute per file belonging to the bulk-request
    //The persistency consists of creating the directory, and set the extended attribute representing each file
    //The key of the extended attribute will be the fid of the file, the value will be the eventual error that
    //a file can have (prepare submission error...)
    eos_debug("msg=\"Persistence of the bulk request %s : creating the directory %s\"",
              bulkRequest->getId().c_str(),
              directoryBulkReqPath.c_str());
    createBulkRequestDirectory(bulkRequest, directoryBulkReqPath);
    eos_debug("msg=\"Persistence of the bulk request %s : creating the xattrs map from the bulk-request paths\"",
              bulkRequest->getId().c_str());
    eos::IContainerMD::XAttrMap xattrs;
    generateXattrsMapFromBulkRequest(bulkRequest, xattrs);
    eos_debug("msg=\"Persistence of the bulk request %s : persisting the bulk-request information in the directory %s\"",
              bulkRequest->getId().c_str(), directoryBulkReqPath.c_str());
    persistBulkRequestDirectory(directoryBulkReqPath, xattrs);
  } catch (const PersistencyException& ex) {
    cleanAfterExceptionHappenedDuringBulkRequestSave(directoryBulkReqPath);
    throw ex;
  }
}

void ProcDirectoryBulkRequestDAO::saveBulkRequest(const CancellationBulkRequest*
    bulkRequest)
{
  cancelStageBulkRequest(bulkRequest);
}

void ProcDirectoryBulkRequestDAO::cancelStageBulkRequest(
  const CancellationBulkRequest* bulkRequest)
{
  std::string bulkRequestProcPath = generateBulkRequestProcPath(bulkRequest);

  if (bulkRequest->getFiles()->size() == 0) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::cancelStageBulkRequest(), unable to cancel the bulk-request id="
        << bulkRequest->getId() << " because it does not contain any files";
    throw PersistencyException(oss.str());
  }

  eos::IContainerMD::XAttrMap xattrs;
  generateXattrsMapFromBulkRequest(bulkRequest, xattrs);
  persistBulkRequestDirectory(bulkRequestProcPath, xattrs);
}

void ProcDirectoryBulkRequestDAO::generateXattrsMapFromBulkRequest(
  const BulkRequest* bulkRequest, eos::IContainerMD::XAttrMap& xattrs)
{
  // Set last access time of the bulk-request directory
  std::time_t now = std::time(nullptr);
  std::string nowStr = std::to_string(now);
  xattrs[LAST_ACCESS_TIME_XATTR_NAME] = nowStr;
}

void ProcDirectoryBulkRequestDAO::generateXattrsMapFromBulkRequest(
  const StageBulkRequest* bulkRequest, eos::IContainerMD::XAttrMap& xattrs)
{
  generateXattrsMapFromBulkRequest(static_cast<const BulkRequest*>(bulkRequest),
                                   xattrs);
  xattrs[ISSUER_UID_XATTR_NAME] = std::to_string(bulkRequest->getIssuerVid().uid);
  xattrs[CREATION_TIME_XATTR_NAME] = std::to_string(
                                       bulkRequest->getCreationTime());
  std::map<bulk::File, folly::Future<IFileMDPtr>> filesWithMDFutures;
  const auto files = bulkRequest->getFiles();

  for (auto& file : *files) {
    std::string path = file->getPath();
    std::pair<bulk::File, folly::Future<IFileMDPtr>> itemToInsert(*file,
        mFileSystem->eosView->getFileFut(path , false));
    filesWithMDFutures.emplace(std::move(itemToInsert));
  }

  for (auto& fileMd : filesWithMDFutures) {
    fileMd.second.wait();
  }

  for (auto& fileWithMDFuture : filesWithMDFutures) {
    const std::string currentFilePath = fileWithMDFuture.first.getPath();
    std::shared_ptr<IFileMD> file;
    std::string fid;

    try {
      eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex);
      file = mFileSystem->eosView->getFile(currentFilePath);
      fid = std::to_string(file->getId());
    } catch (const eos::MDException& ex) {
      //The file does not exist, we will store the path with URL encoding
      std::string encodedFilePath =
        eos::common::StringConversion::curl_default_escaped(currentFilePath);
      // curl encoding does not convert dots '.', so we need to do this explicitly
      eos::common::StringConversion::ReplaceStringInPlace(encodedFilePath, ".",
          "%2E");
      fid = encodedFilePath;
    } catch (const std::exception& ex) {
      std::ostringstream errMsg;
      errMsg << "In ProcDirectoryBulkRequestDAO::generateXattrsMapFromBulkRequest(), got a standard exception trying to get informations about the file "
             << currentFilePath << " ExceptionWhat=\"" << ex.what() << "\"";
      throw PersistencyException(errMsg.str());
    }

    //If a potential error has been set for this file, adding it in the value corresponding
    //to this file's extended attribute
    xattrs[FILE_ID_XATTR_KEY_PREFIX + fid] = "";
    auto error = fileWithMDFuture.first.getError();

    if (error) {
      xattrs[FILE_ID_XATTR_KEY_PREFIX + fid] = *error;
    }
  }
}

void ProcDirectoryBulkRequestDAO::persistBulkRequestDirectory(
  const std::string& directoryBulkReqPath,
  const eos::IContainerMD::XAttrMap& xattrs)
{
  std::shared_ptr<eos::IContainerMD> bulkReqDirMd;
  {
    eos::common::RWMutexWriteLock nsLock(mFileSystem->eosViewRWMutex);

    try {
      bulkReqDirMd = mFileSystem->eosView->getContainer(directoryBulkReqPath);

      for (auto& xattr : xattrs) {
        bulkReqDirMd->setAttribute(xattr.first, xattr.second);
      }

      mFileSystem->eosView->updateContainerStore(bulkReqDirMd.get());
    } catch (const eos::MDException& ex) {
      std::ostringstream oss;
      oss << "In ProcDirectoryBulkRequestDAO::persistBulkRequestDirectory(): unable to persist the bulk-request in the directory "
          << directoryBulkReqPath
          << "ExceptionWhat=\"" << ex.what() << "\"";
      throw PersistencyException(oss.str());
    }
  }
}

void ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(
  const BulkRequest* bulkRequest, const std::string& bulkReqProcPath)
{
  XrdOucErrInfo error;
  int directoryCreationRetCode = mFileSystem->_mkdir(bulkReqProcPath.c_str(),
                                 S_IFDIR | S_IRWXU, error, mVid);

  if (directoryCreationRetCode != SFS_OK) {
    std::ostringstream errMsg;
    errMsg << "In ProcDirectoryBulkRequestDAO::createBulkRequestDirectory(), could not create the directory to save the bulk-request id="
           << bulkRequest->getId()
           << " XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    throw PersistencyException(errMsg.str());
  }
}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(
  const BulkRequest* bulkRequest)
{
  return generateBulkRequestProcPath(bulkRequest->getId(),
                                     bulkRequest->getType());
}

std::string ProcDirectoryBulkRequestDAO::generateBulkRequestProcPath(
  const std::string& bulkRequestId, const BulkRequest::Type& type)
{
  return mProcDirectoryBulkRequestLocations.getDirectoryPathWhereBulkRequestCouldBeSaved(
           type) + bulkRequestId;
}

/*
void ProcDirectoryBulkRequestDAO::insertBulkRequestFilesToBulkRequestDirectory(const BulkRequest * bulkRequest, const std::string & bulkReqProcPath) {
  const auto & files = *bulkRequest->getFiles();
  //Map of files associated to the future object for the in-memory prefetching of the file informations
  std::map<bulk::File,folly::Future<IFileMDPtr>> filesWithMDFutures;
  for(auto & file : files){
    std::string path = file.first;
    std::pair<bulk::File,folly::Future<IFileMDPtr>> itemToInsert(*file.second,mFileSystem->eosView->getFileFut(path , false));
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
      eos::common::RWMutexReadLock nsLock(mFileSystem->eosViewRWMutex);
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
          eos::common::RWMutexWriteLock(mFileSystem->eosViewRWMutex);
          auto fmd = mFileSystem->eosView->createFile(pathOfFileToTouch.str(),
                                                      vid.uid, vid.gid);
          fmd->setSize(0);
          if (fileWithMDFuture.first.getError()) {
            fmd->setAttribute(ERROR_MSG_XATTR_NAME,
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
 */

void ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave(
  const std::string& bulkReqProcPath) noexcept
{
  try {
    deleteDirectory(bulkReqProcPath);
  } catch (const PersistencyException& ex) {
    std::ostringstream debugMsg;
    debugMsg <<
             "In ProcDirectoryBulkRequestDAO::cleanAfterExceptionHappenedDuringBulkRequestSave() "
             << "unable to clean the directory " << bulkReqProcPath << "ErrorMsg=\"" <<
             ex.what() << "\"";
    eos_debug(debugMsg.str().c_str());
  }
}

void ProcDirectoryBulkRequestDAO::deleteDirectory(const std::string& path)
{
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

std::unique_ptr<BulkRequest> ProcDirectoryBulkRequestDAO::getBulkRequest(
  const std::string& id, const BulkRequest::Type& type)
{
  std::unique_ptr<BulkRequest> bulkRequest = nullptr;
  std::string bulkRequestProcPath = this->generateBulkRequestProcPath(id, type);

  try {
    if (existsAndIsDirectory(bulkRequestProcPath)) {
      // Directory exists, the bulk-request can be fetched
      // Update the last access time of the bulk-request directory
      updateLastAccessTime(bulkRequestProcPath);
      //Get all the extended attributes of the directory
      eos::IContainerMD::XAttrMap xattrs;
      fetchExtendedAttributes(bulkRequestProcPath, xattrs);

      switch (type) {
      case BulkRequest::PREPARE_STAGE: {
        bulkRequest = initializeStageBulkRequestFromXattrs(id, xattrs);
        break;
      }

      default:
        std::stringstream ss;
        ss << "The bulk-request has a type (" << BulkRequest::bulkRequestTypeToString(
             type) << ") that cannot be persisted";
        throw PersistencyException(ss.str());
      }
    }
  } catch (const PersistencyException& ex) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::getBulkRequest(): unable to get the bulk request from the persistency layer "
        << "ErrorMsg=\"" << ex.what() << "\"";
    throw PersistencyException(oss.str());
  }

  return bulkRequest;
}

bool ProcDirectoryBulkRequestDAO::existsAndIsDirectory(const std::string&
    dirPath)
{
  XrdOucErrInfo error;
  XrdSfsFileExistence fileExistence;
  int retCode = mFileSystem->_exists(dirPath.c_str(), fileExistence, error, mVid);

  if (retCode != SFS_OK) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::existsAndIsDirectory(), could not get information about the existence of the directory "
        << dirPath << " XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }

  return fileExistence == XrdSfsFileExistIsDirectory;
}

std::unique_ptr<StageBulkRequest>
ProcDirectoryBulkRequestDAO::initializeStageBulkRequestFromXattrs(
  const std::string& requestId, const eos::IContainerMD::XAttrMap& xattrs)
{
  common::VirtualIdentity vid;
  time_t creationTime;

  try {
    vid.uid = ::strtoul(xattrs.at(ISSUER_UID_XATTR_NAME).c_str(), nullptr, 0);
    creationTime = ::strtoul(xattrs.at(CREATION_TIME_XATTR_NAME).c_str(), nullptr,
                             0);
  } catch (const std::out_of_range& ex) {
    throw PersistencyException("Unable to fetch the attributes to create the stage bulk-request");
  }

  std::unique_ptr<StageBulkRequest> stageBulkRequest =
    BulkRequestFactory::createStageBulkRequest(requestId, vid, creationTime);
  fillBulkRequestFromXattrs(stageBulkRequest.get(), xattrs);
  return stageBulkRequest;
}

void ProcDirectoryBulkRequestDAO::fillBulkRequestFromXattrs(
  bulk::BulkRequest* bulkRequest, const eos::IContainerMD::XAttrMap& xattrs)
{
  XrdOucErrInfo errFind;
  XrdOucString stdErr;
  std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>>
      filesInBulkReqProcDirWithFuture;
  std::vector<std::string> fileWithPaths;

  for (auto& fileIdInfos : xattrs) {
    std::string fileIdOrObfuscatedPath = fileIdInfos.first;
    std::optional<std::string> currentFileError;
    size_t pos = fileIdOrObfuscatedPath.find(FILE_ID_XATTR_KEY_PREFIX, 0);

    if (pos != std::string::npos) {
      //We have a file, get its potential error
      fileIdOrObfuscatedPath.erase(0, FILE_ID_XATTR_KEY_PREFIX.length());

      //The error is stored in the value assciated to the key FILE_ID_XATTR_KEY_PREFIX
      if (!fileIdInfos.second.empty()) {
        currentFileError = fileIdInfos.second;
      }
    } else {
      continue;
    }

    //The files in the bulk-request proc directory will be wrapped into a ProcDirBulkRequestFile object.
    ProcDirBulkRequestFile file(fileIdOrObfuscatedPath);

    if (currentFileError) {
      file.setError(*currentFileError);
    }

    try {
      //The file name is normally a fid. But if the file submitted before did not exist, the path will be stored in another format (e.g: URL encoding)
      eos::common::FileId::fileid_t fid = std::stoull(file.getName());
      file.setFileId(fid);
      initiateFileMDFetch(file, filesInBulkReqProcDirWithFuture);
    } catch (std::invalid_argument& ex) {
      // The current file is not a fid, it is therefore a file that has the URL encoding
      // It may also have the old format #:#eos#:#test#:#testFile.txt (#:# replaced by '/')
      std::string filePathCopy = file.getName();

      if (filePathCopy.find("#:#") != std::string::npos) {
        // TODO: Remove this once no more bulk requests exist with the format #:#eos#:#test#:#testFile.txt
        common::StringConversion::ReplaceStringInPlace(filePathCopy, "#:#", "/");
      } else {
        filePathCopy = eos::common::StringConversion::curl_default_unescaped(
                         filePathCopy);
      }

      std::unique_ptr<File> bulkRequestFile = std::make_unique<File>(filePathCopy);
      bulkRequestFile->setError(file.getError());
      bulkRequest->addFile(std::move(bulkRequestFile));
    }
  }

  getFilesPathAndAddToBulkRequest(filesInBulkReqProcDirWithFuture, bulkRequest);
}

void ProcDirectoryBulkRequestDAO::getDirectoryContent(const std::string& path,
    std::map<std::string, std::set<std::string>>& directoryContent)
{
  XrdOucErrInfo error;
  XrdOucString stdErr;

  if (mFileSystem->_find(path.c_str(), error, stdErr, mVid,
                         directoryContent) == SFS_ERROR) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::getDirectoryContent(), could not list the content of the directory "
        << path << " XrdOfsErrMsg=" << error.getErrText();
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }

  //Drop the top directory: it does not belong to its content
  directoryContent.erase(path);
}

void ProcDirectoryBulkRequestDAO::fetchExtendedAttributes(
  const std::string& path, eos::IContainerMD::XAttrMap& xattrs)
{
  XrdOucErrInfo error;

  if (mFileSystem->_attr_ls(path.c_str(), error, mVid, nullptr,
                            xattrs) == SFS_ERROR) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::fetchExtendedAttributes() Unable to get the extended attribute of the file "
        << path << " XrdOfsErrMsg=" << error.getErrText();
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
}

void ProcDirectoryBulkRequestDAO::initiateFileMDFetch(const
    ProcDirBulkRequestFile& file,
    std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>>& filesWithFuture)
{
  std::pair<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>> fileWithFuture(
        file, mFileSystem->eosFileService->getFileMDFut(file.getFileId().value()));
  filesWithFuture.emplace(std::move(fileWithFuture));
}

void ProcDirectoryBulkRequestDAO::getFilesPathAndAddToBulkRequest(
  std::map<ProcDirBulkRequestFile, folly::Future<IFileMDPtr>>& filesWithFuture,
  BulkRequest* bulkRequest)
{
  eos::common::RWMutexReadLock lock(mFileSystem->eosViewRWMutex);

  for (auto& fileWithFuture : filesWithFuture) {
    try {
      fileWithFuture.second.wait();
      std::shared_ptr<IFileMD> fmd = mFileSystem->eosFileService->getFileMD(
                                       fileWithFuture.first.getFileId().value());
      std::unique_ptr<File> bulkReqFile = std::make_unique<File>
                                          (mFileSystem->eosView->getUri(fmd.get()));
      bulkReqFile->setError(fileWithFuture.first.getError());
      bulkRequest->addFile(std::move(bulkReqFile));
    } catch (const eos::MDException& ex) {
      //We could not get any information about this file (might have been deleted for example)
      //log this as a warning and remove this file
      std::stringstream ss;
      ss << "In ProcDirectoryBulkRequestDAO::getFilesPathAndAddToBulkRequest(), unable to get the metadata of the file id="
         << fileWithFuture.first.getFileId().value()
         << " ErrorMsg=\"" << ex.what() << "\"";
      eos_warning(ss.str().c_str());
    }
  }
}

uint64_t ProcDirectoryBulkRequestDAO::deleteBulkRequestNotQueriedFor(
  const BulkRequest::Type& type, const std::chrono::seconds& seconds)
{
  std::string bulkRequestsPath =
    mProcDirectoryBulkRequestLocations.getDirectoryPathWhereBulkRequestCouldBeSaved(
      type);
  std::map<std::string, std::set<std::string>> allBulkRequestDirectories;
  getDirectoryContent(bulkRequestsPath, allBulkRequestDirectories);
  //Now get the last access time of each directory
  std::set<std::string> bulkRequestDirectoriesToDelete;
  uint64_t nbDeletedBulkRequests = 0;

  for (auto& kv : allBulkRequestDirectories) {
    eos::IContainerMD::XAttrMap xattrs;
    fetchExtendedAttributes(kv.first, xattrs);

    try {
      std::string lastAccessTimeStr = xattrs.at(LAST_ACCESS_TIME_XATTR_NAME);
      std::time_t lastAccessTime = std::atoi(lastAccessTimeStr.c_str());
      time_t elapsedTimeBetweenNowAndLastAccessTime = std::time(
            nullptr) - lastAccessTime;

      if (elapsedTimeBetweenNowAndLastAccessTime > seconds.count()) {
        deleteDirectory(kv.first);
        nbDeletedBulkRequests++;
        eos_info("msg=\"Deleted a bulk request from the /proc/ persistency\" path=\"%s\"",
                 kv.first.c_str());
      }
    } catch (const std::out_of_range&) {
      //The extended attribute LAST_ACCESS_TIME_XATTR_NAME was not found, log an error
      eos_err("In ProcDirectoryBulkRequestDAO::deleteBulkRequestNotQueriedFor(), the directory %s does not have the %s extended attribute set. "
              "Unable to know if it can be deleted or not.", kv.first.c_str(),
              LAST_ACCESS_TIME_XATTR_NAME);
    }
  }

  return nbDeletedBulkRequests;
}

void ProcDirectoryBulkRequestDAO::setExtendedAttribute(const std::string& path,
    const std::string& xattrName, const std::string& xattrValue)
{
  XrdOucErrInfo error;
  int retAttrSet = mFileSystem->_attr_set(path.c_str(), error, mVid, nullptr,
                                          xattrName.c_str(), xattrValue.c_str());

  if (retAttrSet != SFS_OK) {
    std::ostringstream oss;
    oss << "In ProcDirectoryBulkRequestDAO::setExtendedAttribute(), could not set the extended attribute "
        << xattrName << " to the file path "
        << path << " XrdOfsErrMsg=\"" << error.getErrText() << "\"";
    eos_err(oss.str().c_str());
    throw PersistencyException(oss.str());
  }
}

void ProcDirectoryBulkRequestDAO::updateLastAccessTime(const std::string& path)
{
  std::time_t now = std::time(nullptr);
  std::string nowStr = std::to_string(now);
  setExtendedAttribute(path, LAST_ACCESS_TIME_XATTR_NAME, nowStr);
}

bool ProcDirectoryBulkRequestDAO::exists(const std::string& bulkRequestId,
    const BulkRequest::Type& type)
{
  std::string bulkRequestPath = generateBulkRequestProcPath(bulkRequestId, type);
  return existsAndIsDirectory(bulkRequestPath);
}

void ProcDirectoryBulkRequestDAO::deleteBulkRequest(const BulkRequest*
    bulkRequest)
{
  std::string bulkRequestPath = generateBulkRequestProcPath(bulkRequest);
  deleteDirectory(bulkRequestPath);
}

EOSBULKNAMESPACE_END
